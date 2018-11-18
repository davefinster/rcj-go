package api

import (
	sq "github.com/Masterminds/squirrel"
	sheetStore "github.com/davefinster/rcj-go/api/sheets"
	crdbStore "github.com/davefinster/rcj-go/api/store/cockroach"
	"github.com/gin-gonic/gin"
	"github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/jmoiron/sqlx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	// For postgres support
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/proto"
	_ "github.com/lib/pq"
	"github.com/shopspring/decimal"
	"golang.org/x/sync/errgroup"
	"io/ioutil"
	"net/http"
	"os"
	"sort"

	"archive/zip"
	serv "github.com/davefinster/rcj-go/api/proto"
	"github.com/improbable-eng/grpc-web/go/grpcweb"
	"github.com/tealeg/xlsx"
)

type robocupGrpcServer struct {
	Store  *crdbStore.CockroachStore
	Sheets *sheetStore.SheetStore
}

func (s *robocupGrpcServer) GetScoreSheetTemplates(ctx context.Context, req *serv.GetScoreSheetTemplatesRequest) (*serv.GetScoreSheetTemplatesResponse, error) {
	templates, err := s.Store.FetchScoreSheetTemplates(ctx, nil)
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "Internal error encountered whilefetching templates")
	}
	return &serv.GetScoreSheetTemplatesResponse{
		ScoreSheetTemplates: templates,
	}, nil
}

func (s *robocupGrpcServer) GetUsers(ctx context.Context, req *serv.GetUsersRequest) (*serv.GetUsersResponse, error) {
	users, err := s.Store.FetchUsers(ctx)
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "Internal error encountered whilefetching templates")
	}
	return &serv.GetUsersResponse{
		Users: users,
	}, nil
}

var dbObj *sqlx.DB

// Server implements the Robocup APIs
type Server struct {
	GRPC           *robocupGrpcServer
	Engine         *gin.Engine
	PostgresString string
	DB             *sqlx.DB
	Store          *crdbStore.CockroachStore
}

type danceLadderEntryScore struct {
	Round   int             `json:"round"`
	Average decimal.Decimal `json:"average"`
	Count   int             `json:"count"`
}

type danceLadderEntry struct {
	ID                string                  `db:"id" json:"id"`
	Name              string                  `db:"name" json:"name"`
	Institution       string                  `db:"institution" json:"institution"`
	DivisionID        string                  `db:"division_id" json:"divisionId"`
	Division          string                  `db:"division" json:"division"`
	CompetitionRounds int                     `db:"competition_rounds" json:"competitionRounds"`
	FinalRounds       int                     `db:"final_rounds" json:"finalRounds"`
	Scores            []danceLadderEntryScore `json:"scores"`
	InterviewScore    decimal.Decimal         `json:"interviewScore"`
	BestRound         decimal.Decimal         `json:"bestRound"`
	BestFinal         decimal.Decimal         `json:"bestFinal"`
	RoundTotal        decimal.Decimal         `json:"roundTotal"`
	FinalTotal        decimal.Decimal         `json:"finalTotal"`
}

type scoreCalc struct {
	Team    string          `db:"team"`
	Round   int             `db:"round"`
	Average decimal.Decimal `db:"total"`
	Count   int             `db:"count"`
}

func roundName(roundIndex, compRounds, finalRounds int) string {
	if roundIndex == 0 {
		return "Interview"
	} else if roundIndex <= compRounds {
		return fmt.Sprintf("Performance %d", roundIndex)
	} else {
		return fmt.Sprintf("Final %d", roundIndex-compRounds)
	}
}

func (s *Server) getScoreSheetExcel(c *gin.Context) {
	teamId := c.Param("id")
	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	innerSql, innerArgs, _ := psql.Select(
		"score_sheets.round as round",
		"score_sheet_sections.section as section",
		"AVG(score_sheet_sections.value * score_sheet_template_sections.multiplier) as avg",
	).From("score_sheet_sections").
		Join("score_sheets ON score_sheet_sections.score_sheet = score_sheets.id").
		Join("score_sheet_template_sections ON score_sheet_sections.section = score_sheet_template_sections.id").
		Where(sq.Eq{"score_sheets.team": teamId}).
		GroupBy("score_sheets.team, score_sheets.round, section").
		OrderBy("round, section").ToSql()
	outerSql, _, _ := psql.Select(
		"t1.section as section",
		"score_sheet_template_sections.title as title",
		"score_sheet_template_sections.description as description",
		"t1.round as round",
		"t1.avg as avg",
	).From(fmt.Sprintf("(%s) as t1", innerSql)).
		Join("score_sheet_template_sections ON score_sheet_template_sections.id = t1.section").
		OrderBy("t1.round, score_sheet_template_sections.display_order").ToSql()
	var list []struct {
		Section     string          `db:"section"`
		Title       string          `db:"title"`
		Description string          `db:"description"`
		Round       int             `db:"round"`
		Average     decimal.Decimal `db:"avg"`
	}
	err := s.DB.Select(&list, outerSql, innerArgs...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}
	commentSql, commentArgs, _ := psql.Select(
		"team",
		"round",
		"concat_agg(comments) as comments",
	).From("score_sheets").
		Where(sq.Eq{"score_sheets.team": teamId}).
		GroupBy("team, round").
		OrderBy("round").ToSql()
	var commentList []struct {
		Team     string `db:"team"`
		Round    int    `db:"round"`
		Comments string `db:"comments"`
	}
	err = s.DB.Select(&commentList, commentSql, commentArgs...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}
	// Get team stuff
	teamSql, teamArgs, _ := psql.Select(
		"teams.name as name",
		"institutions.name as institution",
		"divisions.id as division_id",
		"divisions.name as division",
		"divisions.competition_rounds as competition_rounds",
		"divisions.final_rounds as final_rounds",
	).From("teams").
		Join("institutions ON teams.institution = institutions.id").
		Join("divisions ON teams.division = divisions.id").
		Where(sq.Eq{"teams.id": teamId}).ToSql()
	teamData := struct {
		Name              string `db:"name"`
		Institution       string `db:"institution"`
		Division          string `db:"division"`
		DivisionID        string `db:"division_id"`
		CompetitionRounds int    `db:"competition_rounds"`
		FinalRounds       int    `db:"final_rounds"`
	}{}
	err = s.DB.Get(&teamData, teamSql, teamArgs...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}

	ladderQuery := psql.Select(
		"teams.id as id",
		"teams.name as name",
		"institutions.name as institution",
		"divisions.id as division_id",
		"divisions.name as division",
		"divisions.competition_rounds as competition_rounds",
		"divisions.final_rounds as final_rounds").From("teams").
		Join("institutions ON teams.institution = institutions.id").
		Join("divisions ON teams.division = divisions.id").
		Where(sq.Eq{"divisions.id": teamData.DivisionID})
	ladderSql, ladderArgs, _ := ladderQuery.ToSql()
	ladderList := []*danceLadderEntry{}
	err = s.DB.Select(&ladderList, ladderSql, ladderArgs...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}
	innerScoreSql, _, _ := psql.Select(
		"score_sheets.id as id",
		"SUM(score_sheet_sections.value * score_sheet_template_sections.multiplier) as total",
	).From("score_sheets").
		Join("score_sheet_sections ON score_sheet_sections.score_sheet = score_sheets.id").
		Join("score_sheet_template_sections ON score_sheet_sections.section = score_sheet_template_sections.id").
		GroupBy("score_sheets.id").ToSql()
	scoreQuery := psql.Select(
		"score_sheets.team as team",
		"score_sheets.round as round",
		"AVG(t1.total) as total",
		"count(*) as count",
	).From(fmt.Sprintf("(%s) as t1", innerScoreSql)).Join("score_sheets ON score_sheets.id = t1.id").
		GroupBy("score_sheets.team", "score_sheets.round")
	scoreSql, scoreArgs, _ := scoreQuery.ToSql()
	scores := []scoreCalc{}
	err = s.DB.Select(&scores, scoreSql, scoreArgs...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}
	for _, score := range scores {
		// Find the team match
		for _, team := range ladderList {
			if team.ID != score.Team {
				continue
			}
			teamScore := danceLadderEntryScore{
				Round:   score.Round,
				Average: score.Average.Round(2),
				Count:   score.Count,
			}
			team.Scores = append(team.Scores, teamScore)
		}
	}
	for _, team := range ladderList {
		interviewScore := decimal.Decimal{}
		bestRound := decimal.Decimal{}
		bestFinal := decimal.Decimal{}
		for _, score := range team.Scores {
			if score.Round == 0 {
				interviewScore = interviewScore.Add(score.Average)
			} else if score.Round <= team.CompetitionRounds {
				if score.Average.GreaterThan(bestRound) {
					bestRound = score.Average
				}
			} else if score.Round > team.CompetitionRounds {
				if score.Average.GreaterThan(bestFinal) {
					bestFinal = score.Average
				}
			}
		}
		team.InterviewScore = interviewScore
		team.BestRound = bestRound
		team.BestFinal = bestFinal
		team.RoundTotal = interviewScore.Add(bestRound)
		if team.BestFinal.GreaterThan(decimal.Decimal{}) {
			team.FinalTotal = interviewScore.Add(bestFinal)
		} else {
			team.FinalTotal = decimal.Decimal{}
		}
	}
	sort.SliceStable(ladderList, func(i, j int) bool {
		if ladderList[i].FinalTotal.LessThan(ladderList[j].FinalTotal) {
			return true
		}
		if ladderList[i].FinalTotal.GreaterThan(ladderList[j].FinalTotal) {
			return false
		}
		if ladderList[i].RoundTotal.LessThan(ladderList[j].RoundTotal) {
			return true
		}
		if ladderList[i].RoundTotal.GreaterThan(ladderList[j].RoundTotal) {
			return false
		}
		return ladderList[i].Name < ladderList[j].Name
	})
	reversedLadder := make([]*danceLadderEntry, len(ladderList))
	for idx, ladderItem := range ladderList {
		reversedLadder[len(ladderList)-idx-1] = ladderItem
	}
	// One tab per round and match up the comments
	file := xlsx.NewFile()
	ladderSheet, _ := file.AddSheet("Ladder")
	ladderHeaderRow := ladderSheet.AddRow()
	teamName := ladderHeaderRow.AddCell()
	teamName.Value = "Team Name (School Name)"
	roundsScore := ladderHeaderRow.AddCell()
	roundsScore.Value = "Score After Rounds"
	finalsScore := ladderHeaderRow.AddCell()
	finalsScore.Value = "Score After Finals"
	for _, team := range reversedLadder {
		ladderRow := ladderSheet.AddRow()
		teamN := ladderRow.AddCell()
		teamN.Value = fmt.Sprintf("%s (%s)", team.Name, team.Institution)
		rounds := ladderRow.AddCell()
		rounds.Value = team.RoundTotal.Round(2).String()
		finals := ladderRow.AddCell()
		finals.Value = team.FinalTotal.Round(2).String()
	}
	for i := 0; i < teamData.CompetitionRounds+teamData.FinalRounds; i++ {
		sheet, _ := file.AddSheet(roundName(i, teamData.CompetitionRounds, teamData.FinalRounds))
		headerRow := sheet.AddRow()
		title := headerRow.AddCell()
		title.Value = "Criteria"
		desc := headerRow.AddCell()
		desc.Value = "Description"
		avg := headerRow.AddCell()
		avg.Value = "Average"
	}
	totalMap := map[int]decimal.Decimal{}
	for _, entry := range list {
		sheet := file.Sheet[roundName(entry.Round, teamData.CompetitionRounds, teamData.FinalRounds)]
		row := sheet.AddRow()
		title := row.AddCell()
		title.Value = entry.Title
		desc := row.AddCell()
		desc.Value = entry.Description
		avg := row.AddCell()
		avg.Value = entry.Average.Round(2).String()
		totalMap[entry.Round] = totalMap[entry.Round].Add(entry.Average)
	}
	for i := 0; i < teamData.CompetitionRounds+teamData.FinalRounds; i++ {
		sheet := file.Sheet[roundName(i, teamData.CompetitionRounds, teamData.FinalRounds)]
		row := sheet.AddRow()
		title := row.AddCell()
		title.Value = ""
		desc := row.AddCell()
		desc.Value = "Total:"
		avg := row.AddCell()
		avg.Value = totalMap[i].Round(2).String()
	}
	for _, commentEntry := range commentList {
		sheet := file.Sheet[roundName(commentEntry.Round, teamData.CompetitionRounds, teamData.FinalRounds)]
		commentRow := sheet.AddRow()
		comment := commentRow.AddCell()
		comment.Value = "Comments"
		comments := commentRow.AddCell()
		comments.Value = commentEntry.Comments
	}
	c.Status(http.StatusOK)
	header := c.Writer.Header()
	header["Content-type"] = []string{"application/octet-stream"}
	header["Content-Disposition"] = []string{fmt.Sprintf("attachment; filename=%s-%s.xlsx", teamData.Name, teamData.Institution)}
	file.Write(c.Writer)
}

func (s *Server) getScoreSheetExcelForDivision(c *gin.Context) {
	divisionId := c.Param("id")
	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	innerSql, innerArgs, _ := psql.Select(
		"score_sheets.team as team",
		"score_sheets.round as round",
		"score_sheet_sections.section as section",
		"AVG(score_sheet_sections.value * score_sheet_template_sections.multiplier) as avg",
	).From("score_sheet_sections").
		Join("score_sheets ON score_sheet_sections.score_sheet = score_sheets.id").
		Join("score_sheet_template_sections ON score_sheet_sections.section = score_sheet_template_sections.id").
		Where(sq.Eq{"score_sheets.division": divisionId}).
		GroupBy("score_sheets.team, score_sheets.round, section").
		OrderBy("round, section").ToSql()
	outerSql, _, _ := psql.Select(
		"t1.team as team",
		"t1.section as section",
		"score_sheet_template_sections.title as title",
		"score_sheet_template_sections.description as description",
		"t1.round as round",
		"t1.avg as avg",
	).From(fmt.Sprintf("(%s) as t1", innerSql)).
		Join("score_sheet_template_sections ON score_sheet_template_sections.id = t1.section").
		OrderBy("t1.round, score_sheet_template_sections.display_order").ToSql()
	var list []struct {
		Team        string          `db:"team"`
		Section     string          `db:"section"`
		Title       string          `db:"title"`
		Description string          `db:"description"`
		Round       int             `db:"round"`
		Average     decimal.Decimal `db:"avg"`
	}
	err := s.DB.Select(&list, outerSql, innerArgs...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}
	commentSql, commentArgs, _ := psql.Select(
		"team",
		"round",
		"concat_agg(comments) as comments",
	).From("score_sheets").
		Where(sq.Eq{"division": divisionId}).
		GroupBy("team, round").
		OrderBy("round").ToSql()
	var commentList []struct {
		Team     string `db:"team"`
		Round    int    `db:"round"`
		Comments string `db:"comments"`
	}
	err = s.DB.Select(&commentList, commentSql, commentArgs...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}
	// Get team stuff
	teamSql, teamArgs, _ := psql.Select(
		"teams.id as id",
		"teams.name as name",
		"institutions.name as institution",
		"divisions.id as division_id",
		"divisions.name as division",
		"divisions.competition_rounds as competition_rounds",
		"divisions.final_rounds as final_rounds",
	).From("teams").
		Join("institutions ON teams.institution = institutions.id").
		Join("divisions ON teams.division = divisions.id").
		Where(sq.Eq{"teams.division": divisionId}).ToSql()
	var teamData []struct {
		ID                string `db:"id"`
		Name              string `db:"name"`
		Institution       string `db:"institution"`
		Division          string `db:"division"`
		DivisionID        string `db:"division_id"`
		CompetitionRounds int    `db:"competition_rounds"`
		FinalRounds       int    `db:"final_rounds"`
	}
	err = s.DB.Select(&teamData, teamSql, teamArgs...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}

	ladderQuery := psql.Select(
		"teams.id as id",
		"teams.name as name",
		"institutions.name as institution",
		"divisions.id as division_id",
		"divisions.name as division",
		"divisions.competition_rounds as competition_rounds",
		"divisions.final_rounds as final_rounds").From("teams").
		Join("institutions ON teams.institution = institutions.id").
		Join("divisions ON teams.division = divisions.id").
		Where(sq.Eq{"divisions.id": divisionId})
	ladderSql, ladderArgs, _ := ladderQuery.ToSql()
	ladderList := []*danceLadderEntry{}
	err = s.DB.Select(&ladderList, ladderSql, ladderArgs...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}
	innerScoreSql, _, _ := psql.Select(
		"score_sheets.id as id",
		"SUM(score_sheet_sections.value * score_sheet_template_sections.multiplier) as total",
	).From("score_sheets").
		Join("score_sheet_sections ON score_sheet_sections.score_sheet = score_sheets.id").
		Join("score_sheet_template_sections ON score_sheet_sections.section = score_sheet_template_sections.id").
		GroupBy("score_sheets.id").ToSql()
	scoreQuery := psql.Select(
		"score_sheets.team as team",
		"score_sheets.round as round",
		"AVG(t1.total) as total",
		"count(*) as count",
	).From(fmt.Sprintf("(%s) as t1", innerScoreSql)).Join("score_sheets ON score_sheets.id = t1.id").
		GroupBy("score_sheets.team", "score_sheets.round")
	scoreSql, scoreArgs, _ := scoreQuery.ToSql()
	scores := []scoreCalc{}
	err = s.DB.Select(&scores, scoreSql, scoreArgs...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}
	for _, score := range scores {
		// Find the team match
		for _, team := range ladderList {
			if team.ID != score.Team {
				continue
			}
			teamScore := danceLadderEntryScore{
				Round:   score.Round,
				Average: score.Average.Round(2),
				Count:   score.Count,
			}
			team.Scores = append(team.Scores, teamScore)
		}
	}
	for _, team := range ladderList {
		interviewScore := decimal.Decimal{}
		bestRound := decimal.Decimal{}
		bestFinal := decimal.Decimal{}
		for _, score := range team.Scores {
			if score.Round == 0 {
				interviewScore = interviewScore.Add(score.Average)
			} else if score.Round <= team.CompetitionRounds {
				if score.Average.GreaterThan(bestRound) {
					bestRound = score.Average
				}
			} else if score.Round > team.CompetitionRounds {
				if score.Average.GreaterThan(bestFinal) {
					bestFinal = score.Average
				}
			}
		}
		team.InterviewScore = interviewScore
		team.BestRound = bestRound
		team.BestFinal = bestFinal
		team.RoundTotal = interviewScore.Add(bestRound)
		if team.BestFinal.GreaterThan(decimal.Decimal{}) {
			team.FinalTotal = interviewScore.Add(bestFinal)
		} else {
			team.FinalTotal = decimal.Decimal{}
		}
	}
	sort.SliceStable(ladderList, func(i, j int) bool {
		if ladderList[i].FinalTotal.LessThan(ladderList[j].FinalTotal) {
			return true
		}
		if ladderList[i].FinalTotal.GreaterThan(ladderList[j].FinalTotal) {
			return false
		}
		if ladderList[i].RoundTotal.LessThan(ladderList[j].RoundTotal) {
			return true
		}
		if ladderList[i].RoundTotal.GreaterThan(ladderList[j].RoundTotal) {
			return false
		}
		return ladderList[i].Name < ladderList[j].Name
	})
	reversedLadder := make([]*danceLadderEntry, len(ladderList))
	for idx, ladderItem := range ladderList {
		reversedLadder[len(ladderList)-idx-1] = ladderItem
	}

	zipBuf := new(bytes.Buffer)
	w := zip.NewWriter(zipBuf)

	divisionName := ""

	for _, currentTeam := range teamData {
		divisionName = currentTeam.Division
		// One tab per round and match up the comments
		file := xlsx.NewFile()
		ladderSheet, _ := file.AddSheet("Ladder")
		ladderHeaderRow := ladderSheet.AddRow()
		teamName := ladderHeaderRow.AddCell()
		teamName.Value = "Team Name (School Name)"
		roundsScore := ladderHeaderRow.AddCell()
		roundsScore.Value = "Score After Rounds"
		finalsScore := ladderHeaderRow.AddCell()
		finalsScore.Value = "Score After Finals"
		for _, team := range reversedLadder {
			ladderRow := ladderSheet.AddRow()
			teamN := ladderRow.AddCell()
			teamN.Value = fmt.Sprintf("%s (%s)", team.Name, team.Institution)
			rounds := ladderRow.AddCell()
			rounds.Value = team.RoundTotal.Round(2).String()
			finals := ladderRow.AddCell()
			finals.Value = team.FinalTotal.Round(2).String()
		}
		totalMap := map[int]decimal.Decimal{}
		for _, entry := range list {
			if entry.Team != currentTeam.ID {
				continue
			}
			sheet := file.Sheet[roundName(entry.Round, currentTeam.CompetitionRounds, currentTeam.FinalRounds)]
			if sheet == nil {
				sheet, _ = file.AddSheet(roundName(entry.Round, currentTeam.CompetitionRounds, currentTeam.FinalRounds))
				headerRow := sheet.AddRow()
				title := headerRow.AddCell()
				title.Value = "Criteria"
				desc := headerRow.AddCell()
				desc.Value = "Description"
				avg := headerRow.AddCell()
				avg.Value = "Average"
			}
			row := sheet.AddRow()
			title := row.AddCell()
			title.Value = entry.Title
			desc := row.AddCell()
			desc.Value = entry.Description
			avg := row.AddCell()
			avg.Value = entry.Average.Round(2).String()
			totalMap[entry.Round] = totalMap[entry.Round].Add(entry.Average)
		}
		for key, value := range totalMap {
			sheet := file.Sheet[roundName(key, currentTeam.CompetitionRounds, currentTeam.FinalRounds)]
			row := sheet.AddRow()
			title := row.AddCell()
			title.Value = ""
			desc := row.AddCell()
			desc.Value = "Total:"
			avg := row.AddCell()
			avg.Value = value.Round(2).String()
		}
		for _, commentEntry := range commentList {
			if commentEntry.Team != currentTeam.ID {
				continue
			}
			sheet := file.Sheet[roundName(commentEntry.Round, currentTeam.CompetitionRounds, currentTeam.FinalRounds)]
			commentRow := sheet.AddRow()
			comment := commentRow.AddCell()
			comment.Value = "Comments"
			comments := commentRow.AddCell()
			comments.Value = commentEntry.Comments
		}
		fileName := fmt.Sprintf("%s-%s.xlsx", currentTeam.Name, currentTeam.Institution)
		f, _ := w.Create(fileName)
		file.Write(f)
	}

	c.Status(http.StatusOK)
	header := c.Writer.Header()
	header["Content-type"] = []string{"application/octet-stream"}
	header["Content-Disposition"] = []string{fmt.Sprintf("attachment; filename=%s.zip", divisionName)}
	w.Close()
	c.Writer.Write(zipBuf.Bytes())
}

type fullUser struct {
	ID       string `db:"id" json:"id"`
	Name     string `db:"name" json:"name"`
	Username string `db:"username" json:"username"`
	IsAdmin  bool   `db:"is_admin" json:"isAdmin"`
	Password string `db:"hashed_password" json:"-"`
}

type counter struct {
	UserCount int `db:"user_count"`
}

func (s *Server) Authenticate() gin.HandlerFunc {
	return func(c *gin.Context) {
		fmt.Printf("asdf\n")
		psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
		cookie, err := c.Cookie("rcj-auth")
		if err != nil || len(cookie) == 0 {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		sql, args, _ := psql.Select("id", "name", "username", "is_admin").
			From("users").Where(sq.Eq{"id": cookie}).ToSql()
		user := fullUser{}
		err = s.DB.Get(&user, sql, args...)
		if err != nil || user.ID == "" {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		c.Next()
	}
}

func (s *robocupGrpcServer) GetSheetConfig(ctx context.Context, req *serv.GetSheetConfigRequest) (*serv.GetSheetConfigResponse, error) {
	config, err := s.Sheets.GetSpreadsheets()
	if err != nil {
		return nil, err
	}
	return &serv.GetSheetConfigResponse{
		IngestionSpreadsheetId: config.IngestionSpreadsheetID,
		PushSpreadsheetId:      config.PushSpreadsheetID,
	}, nil
}

func (s *robocupGrpcServer) SubmitSheetConfig(ctx context.Context, req *serv.SubmitSheetConfigRequest) (*serv.SubmitSheetConfigResponse, error) {
	if req.GetCode() != "" {
		err := s.Sheets.SetCode(req.GetCode())
		if err != nil {
			return nil, err
		}
	}
	if req.GetIngestionSpreadsheetId() != "" {
		err := s.Sheets.SetIngestionSpreadsheet(req.GetIngestionSpreadsheetId())
		if err != nil {
			return nil, err
		}
	}
	if req.GetPushSpreadsheetId() != "" {
		err := s.Sheets.SetPushSpreadsheet(req.GetPushSpreadsheetId())
		if err != nil {
			return nil, err
		}
	}
	return &serv.SubmitSheetConfigResponse{}, nil
}

func (s *robocupGrpcServer) GetSheetAuthUrl(ctx context.Context, req *serv.GetSheetAuthUrlRequest) (*serv.GetSheetAuthUrlResponse, error) {
	url, err := s.Sheets.AuthURL()
	if err != nil {
		return nil, err
	}
	return &serv.GetSheetAuthUrlResponse{
		Url: url,
	}, nil
}

func (s *robocupGrpcServer) SyncCheckins(ctx context.Context, req *serv.SyncCheckinsRequest) (*serv.SyncCheckinsResponse, error) {
	checkins, err := s.Store.FetchCheckins(ctx)
	if err != nil {
		return nil, err
	}
	teams, err := s.Store.FetchTeams(ctx, nil)
	if err != nil {
		return nil, err
	}
	divisions, err := s.Store.FetchDivisions(ctx, nil)
	if err != nil {
		return nil, err
	}
	err = s.Sheets.ExportToSheets(checkins, teams, divisions)
	if err != nil {
		return nil, err
	}
	return &serv.SyncCheckinsResponse{}, nil
}

func (s *robocupGrpcServer) Login(ctx context.Context, req *serv.LoginRequest) (*serv.LoginResponse, error) {
	user, err := s.Store.AuthenticateUserWithCredentials(req.GetUsername(), req.GetPassword())
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "Internal error encountered while performing login")
	}
	if user == nil {
		return nil, grpc.Errorf(codes.Unauthenticated, "Invalid credentials")
	}
	cookie := http.Cookie{
		Name:     "rcj-auth",
		Value:    user.Id,
		Path:     "/",
		HttpOnly: true,
		MaxAge:   0,
	}
	header := metadata.Pairs("set-cookie", cookie.String())
	grpc.SendHeader(ctx, header)
	return &serv.LoginResponse{
		AuthenticatedUser: user,
	}, nil
}

func (s *robocupGrpcServer) GetCurrentUser(ctx context.Context, req *serv.GetCurrentUserRequest) (*serv.GetCurrentUserResponse, error) {
	meta, _ := metadata.FromIncomingContext(ctx)
	userIds := meta.Get("user-id")
	user, err := s.Store.FetchUser(userIds[0], nil)
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "Internal error encountered while performing login")
	}
	if user == nil {
		return nil, grpc.Errorf(codes.Unauthenticated, "Invalid credentials")
	}
	return &serv.GetCurrentUserResponse{
		AuthenticatedUser: user,
	}, nil
}

func (s *robocupGrpcServer) GetDanceLadder(ctx context.Context, req *serv.GetDanceLadderRequest) (*serv.GetDanceLadderResponse, error) {
	ladders, err := s.Store.FetchDanceLadders(req.GetShowAll())
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "Internal error encountered while performing login")
	}
	return &serv.GetDanceLadderResponse{
		Divisions: ladders,
	}, nil
}

func (s *robocupGrpcServer) GetDivision(ctx context.Context, req *serv.GetDivisionRequest) (*serv.GetDivisionResponse, error) {
	group, context := errgroup.WithContext(ctx)
	var div *serv.Division
	var teams []*serv.Team
	var scoreSheetTemplates []*serv.ScoreSheetTemplate
	group.Go(func() error {
		fetchedDiv, err := s.Store.FetchDivision(req.GetDivisionId())
		if err != nil {
			return err
		}
		div = fetchedDiv
		fetchedTemplates, err := s.Store.FetchScoreSheetTemplates(context, &crdbStore.FetchScoreSheetTemplateOptions{
			IDs: []string{fetchedDiv.GetInterviewTemplateId(), fetchedDiv.GetPerformanceTemplateId()},
		})
		if err != nil {
			return err
		}
		scoreSheetTemplates = fetchedTemplates
		return nil
	})
	group.Go(func() error {
		divId := req.GetDivisionId()
		fetchedTeams, err := s.Store.FetchTeams(context, &crdbStore.FetchTeamsOptions{
			Division: &divId,
		})
		if err != nil {
			return err
		}
		teams = fetchedTeams
		return nil
	})
	err := group.Wait()
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "Internal error encountered while fetching data")
	}
	return &serv.GetDivisionResponse{
		Division:  div,
		Teams:     teams,
		Templates: scoreSheetTemplates,
	}, nil
}

func (s *robocupGrpcServer) GetScoreSheet(ctx context.Context, req *serv.GetScoreSheetRequest) (*serv.GetScoreSheetResponse, error) {
	sheet, err := s.Store.FetchScoreSheet(ctx, req.GetScoreSheetId(), nil)
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "Internal error encountered while getting score sheet")
	}
	return &serv.GetScoreSheetResponse{
		ScoreSheet: sheet,
	}, nil
}

func (s *robocupGrpcServer) GetCheckins(ctx context.Context, req *serv.GetCheckinsRequest) (*serv.GetCheckinsResponse, error) {
	checkins, err := s.Store.FetchCheckins(ctx)
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "Internal error encountered while getting divisions")
	}
	return &serv.GetCheckinsResponse{
		CheckIns: checkins,
	}, nil
}

func (s *robocupGrpcServer) CreateCheckin(ctx context.Context, req *serv.CreateCheckinRequest) (*serv.CreateCheckinResponse, error) {
	checkin, err := s.Store.CreateCheckin(ctx, func(newCheckin *serv.Checkin) error {
		meta, _ := metadata.FromIncomingContext(ctx)
		userIds := meta.Get("user-id")
		userId := userIds[0]
		proto.Merge(newCheckin, req.GetCheckIn())
		newCheckin.Agent = &serv.User{
			Id: userId,
		}
		return nil
	})
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "Internal error encountered while creating checkin")
	}
	return &serv.CreateCheckinResponse{
		CheckIn: checkin,
	}, nil
}

func (s *robocupGrpcServer) GetDivisions(ctx context.Context, req *serv.GetDivisionsRequest) (*serv.GetDivisionsResponse, error) {
	divs, err := s.Store.FetchDivisions(ctx, nil)
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "Internal error encountered while getting divisions")
	}
	return &serv.GetDivisionsResponse{
		Divisions: divs,
	}, nil
}

func gRPCMiddleware(server *grpcweb.WrappedGrpcServer) gin.HandlerFunc {
	return func(c *gin.Context) {
		if server.IsGrpcWebRequest(c.Request) {
			//c.AbortWithStatus(200)
			c.Abort()
			c.Status(200)
			server.HandleGrpcWebRequest(c.Writer, c.Request)
			return
		}
		fmt.Printf("Not gRPC %+v\n", c.Request.RequestURI)
	}
}

func (s *robocupGrpcServer) UpdateScoreSheet(ctx context.Context, req *serv.UpdateScoreSheetRequest) (*serv.UpdateScoreSheetResponse, error) {
	scoreSheet, err := s.Store.UpdateScoreSheet(ctx, req.ScoreSheet.Id, func(scoreSheet *serv.ScoreSheet) error {
		scoreSheet.Team.Id = req.ScoreSheet.GetTeam().GetId()
		scoreSheet.Timings = req.ScoreSheet.GetTimings()
		scoreSheet.Comments = req.ScoreSheet.GetComments()
		scoreSheet.Sections = req.ScoreSheet.GetSections()
		return nil
	})
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "Internal error encountered while updating score sheet")
	}
	return &serv.UpdateScoreSheetResponse{
		ScoreSheet: scoreSheet,
	}, nil
}

func (s *robocupGrpcServer) CreateScoreSheetTemplate(ctx context.Context, req *serv.CreateScoreSheetTemplateRequest) (*serv.CreateScoreSheetTemplateResponse, error) {
	template, err := s.Store.CreateScoreSheetTemplate(ctx, func(newTemplate *serv.ScoreSheetTemplate) error {
		proto.Merge(newTemplate, req.GetScoreSheetTemplate())
		return nil
	})
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "Internal error encountered while creating score sheet template")
	}
	return &serv.CreateScoreSheetTemplateResponse{
		ScoreSheetTemplate: template,
	}, nil
}

func (s *robocupGrpcServer) CreateScoreSheet(ctx context.Context, req *serv.CreateScoreSheetRequest) (*serv.CreateScoreSheetResponse, error) {
	scoreSheet, err := s.Store.CreateScoreSheet(ctx, func(newScoreSheet *serv.ScoreSheet) error {
		meta, _ := metadata.FromIncomingContext(ctx)
		userIds := meta.Get("user-id")
		userId := userIds[0]
		proto.Merge(newScoreSheet, req.GetScoreSheet())
		newScoreSheet.Author = &serv.User{
			Id: userId,
		}
		return nil
	})
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "Internal error encountered while creating score sheet")
	}
	return &serv.CreateScoreSheetResponse{
		ScoreSheet: scoreSheet,
	}, nil
}

func (s *robocupGrpcServer) CreateUser(ctx context.Context, req *serv.CreateUserRequest) (*serv.CreateUserResponse, error) {
	user, err := s.Store.CreateUser(ctx, func(newUser *serv.User) error {
		proto.Merge(newUser, req.GetUser())
		return nil
	})
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "Internal error encountered while creating user")
	}
	return &serv.CreateUserResponse{
		User: user,
	}, nil
}

func (s *robocupGrpcServer) UpdateUser(ctx context.Context, req *serv.UpdateUserRequest) (*serv.UpdateUserResponse, error) {
	user, err := s.Store.UpdateUser(ctx, req.GetUser().GetId(), func(existingUser *serv.User) error {
		proto.Merge(existingUser, req.GetUser())
		return nil
	})
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "Internal error encountered while updating user")
	}
	return &serv.UpdateUserResponse{
		User: user,
	}, nil
}

func (s *robocupGrpcServer) CreateDivision(ctx context.Context, req *serv.CreateDivisionRequest) (*serv.CreateDivisionResponse, error) {
	division, err := s.Store.CreateDivision(ctx, func(newDivision *serv.Division) error {
		proto.Merge(newDivision, req.GetDivision())
		return nil
	})
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "Internal error encountered while creating division")
	}
	return &serv.CreateDivisionResponse{
		Division: division,
	}, nil
}

func (s *robocupGrpcServer) GetTeams(ctx context.Context, req *serv.GetTeamsRequest) (*serv.GetTeamsResponse, error) {
	opts := &crdbStore.FetchTeamsOptions{
		PopulateMembers: req.GetPopulateMembers(),
	}
	teams, err := s.Store.FetchTeams(ctx, opts)
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "Internal error encountered whilefetching teams")
	}
	return &serv.GetTeamsResponse{
		Teams: teams,
	}, nil
}

func (s *robocupGrpcServer) GetTeam(ctx context.Context, req *serv.GetTeamRequest) (*serv.GetTeamResponse, error) {
	group, _ := errgroup.WithContext(ctx)
	var team *serv.Team
	var div *serv.Division
	group.Go(func() error {
		fetchedTeam, err := s.Store.FetchTeam(ctx, req.GetTeamId(), nil)
		if err != nil {
			return err
		}
		team = fetchedTeam
		fetchedDiv, err := s.Store.FetchDivision(fetchedTeam.GetDivision())
		if err != nil {
			return err
		}
		div = fetchedDiv
		return nil
	})
	var scoreSheets []*serv.ScoreSheet
	group.Go(func() error {
		teamID := req.GetTeamId()
		opts := &crdbStore.FetchScoreSheetSummaryOptions{
			TeamID: &teamID,
		}
		fetchedSheets, err := s.Store.FetchScoreSheetSummary(ctx, opts, nil)
		if err != nil {
			return err
		}
		scoreSheets = fetchedSheets
		return nil
	})
	err := group.Wait()
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "Internal error encountered while updating score sheet")
	}
	return &serv.GetTeamResponse{
		Team:        team,
		Division:    div,
		ScoreSheets: scoreSheets,
	}, nil
}

func (s *robocupGrpcServer) GetScoreSheets(ctx context.Context, req *serv.GetScoreSheetsRequest) (*serv.GetScoreSheetsResponse, error) {
	meta, _ := metadata.FromIncomingContext(ctx)
	userIds := meta.Get("user-id")
	userId := userIds[0]
	opts := &crdbStore.FetchScoreSheetSummaryOptions{
		AuthorID: &userId,
	}
	sheets, err := s.Store.FetchScoreSheetSummary(ctx, opts, nil)
	if err != nil {
		fmt.Printf("%+v", err)
		return nil, grpc.Errorf(codes.Internal, "Internal error encountered while getting score sheets")
	}
	return &serv.GetScoreSheetsResponse{
		ScoreSheets: sheets,
	}, nil
}

func (s *robocupGrpcServer) CreateTeam(ctx context.Context, req *serv.CreateTeamRequest) (*serv.CreateTeamResponse, error) {
	team, err := s.Store.CreateTeam(ctx, nil, func(team *serv.Team) error {
		proto.Merge(team, req.GetTeam())
		return nil
	})
	if err != nil {
		fmt.Printf("%+v", err)
		return nil, grpc.Errorf(codes.Internal, "Internal error encountered while creating team")
	}
	return &serv.CreateTeamResponse{
		Team: team,
	}, nil
}

func (s *robocupGrpcServer) UpdateTeam(ctx context.Context, req *serv.UpdateTeamRequest) (*serv.UpdateTeamResponse, error) {
	team, err := s.Store.UpdateTeam(ctx, req.GetTeam().GetId(), func(team *serv.Team) error {
		team.Name = req.Team.GetName()
		team.Division = req.Team.GetDivision()
		team.Institution = req.Team.Institution
		team.Members = req.Team.Members
		return nil
	})
	if err != nil {
		fmt.Printf("%+v\n", err)
		return nil, grpc.Errorf(codes.Internal, "Internal error encountered while updating team")
	}
	return &serv.UpdateTeamResponse{
		Team: team,
	}, nil
}

func (s *robocupGrpcServer) GetInstitutions(ctx context.Context, req *serv.GetInstitutionsRequest) (*serv.GetInstitutionsResponse, error) {
	insts, err := s.Store.FetchInstitutions(ctx, req.GetSearchString())
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "Internal error encountered while getting institutions")
	}
	return &serv.GetInstitutionsResponse{
		Institutions: insts,
	}, nil
}

func (s *robocupGrpcServer) GetSheetTeams(ctx context.Context, req *serv.GetSheetTeamsRequest) (*serv.GetSheetTeamsResponse, error) {
	teams, err := s.Sheets.TeamsFromSheets()
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "Internal error encountered while getting institutions")
	}
	importIds := []string{}
	for _, team := range teams {
		importIds = append(importIds, team.ImportId)
	}
	existingTeams, err := s.Store.FetchTeams(ctx, &crdbStore.FetchTeamsOptions{
		ImportID: importIds,
	})
	newTeams := []*serv.Team{}
	for _, sheetTeam := range teams {
		match := false
		for _, existingTeam := range existingTeams {
			if existingTeam.GetImportId() == sheetTeam.GetImportId() {
				match = true
				break
			}
		}
		if match {
			continue
		}
		newTeams = append(newTeams, sheetTeam)
	}
	return &serv.GetSheetTeamsResponse{
		Teams: newTeams,
	}, nil
}

func (s *Server) AuthenticateRPC() func(context.Context) (context.Context, error) {
	return func(ctx context.Context) (context.Context, error) {
		psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
		meta, _ := metadata.FromIncomingContext(ctx)
		url := meta.Get("x-original-uri")
		if url[0] == "/Robocup/Login" {
			return ctx, nil
		}
		userId := ""
		rawCookieSet := meta.Get("cookie")
		if len(rawCookieSet) > 0 {
			rawCookies := rawCookieSet[0]
			header := http.Header{}
			header.Add("Cookie", rawCookies)
			request := http.Request{Header: header}
			cook, err := request.Cookie("rcj-auth")
			if cook != nil && err == nil {
				userId = cook.Value
			}
		}
		if userId == "" {
			return nil, grpc.Errorf(codes.Unauthenticated, "Not logged in")
		}
		sql, args, _ := psql.Select("id", "name", "username", "is_admin").
			From("users").Where(sq.Eq{"id": userId}).ToSql()
		var users []fullUser
		err := s.DB.Select(&users, sql, args...)
		if err != nil {
			return nil, grpc.Errorf(codes.Internal, "Error: %+v", err)
		}
		if len(users) == 0 {
			return nil, grpc.Errorf(codes.Unauthenticated, "User id not found")
		}
		userIdMeta := metadata.Pairs("user-id", userId)
		return metadata.NewIncomingContext(ctx, metadata.Join(meta, userIdMeta)), nil
	}
}

type ServerConfig struct {
	AppSecretPath    string `json:"appSecretPath"`
	ConnectionString string `json:"ConnectionString"`
	GinMode          string `json:"ginMode"`
}

var config ServerConfig

// Start boots the RobocupServer
func (s *Server) Start() error {
	filePath := os.Getenv("RCJ_CONFIG_PATH")
	b, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}
	err = json.Unmarshal(b, &config)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Running with config %+v\n", config)
	db, err := sqlx.Connect("postgres", config.ConnectionString)
	if err != nil {
		return err
	}
	if dbObj == nil {
		dbObj = db
	}
	s.DB = dbObj
	store, err := crdbStore.NewCockroachStore(config.ConnectionString)
	if err != nil {
		return err
	}
	s.Store = store
	if config.GinMode == "release" {
		gin.SetMode(gin.ReleaseMode)
	}
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(grpc_auth.UnaryServerInterceptor(s.AuthenticateRPC())),
	)
	sheets := sheetStore.NewSheetStore(config.AppSecretPath)
	sheets.DB = dbObj
	serv.RegisterRobocupServer(grpcServer, &robocupGrpcServer{
		Store:  store,
		Sheets: sheets,
	})
	wrapped := grpcweb.WrapServer(grpcServer)
	s.Engine = gin.Default()
	s.Engine.Use(gRPCMiddleware(wrapped))
	authorised := s.Engine.Group("/api", s.Authenticate())
	authorised.GET("/division/:id/excel", s.getScoreSheetExcelForDivision)
	authorised.GET("/team/:id/excel", s.getScoreSheetExcel)
	return s.Engine.Run()

}
