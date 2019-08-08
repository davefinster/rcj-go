package cockroach

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/davefinster/cockroach-go/crdb"
	rcjpb "github.com/davefinster/rcj-go/api/proto"
	"github.com/elithrar/simple-scrypt"
	tspb "github.com/golang/protobuf/ptypes/timestamp"
	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/types"
	_ "github.com/lib/pq"
	"github.com/shopspring/decimal"
	"golang.org/x/sync/errgroup"
	"sort"
	"strings"
	"sync"
	"time"
)

type CockroachStore struct {
	DB   *sqlx.DB
	PSQL sq.StatementBuilderType
}

func NewCockroachStore(postgres string) (*CockroachStore, error) {
	db, err := sqlx.Connect("postgres", postgres)
	if err != nil {
		return nil, err
	}
	return &CockroachStore{
		DB:   db,
		PSQL: sq.StatementBuilder.PlaceholderFormat(sq.Dollar),
	}, nil
}

func (s *CockroachStore) createInitialUser(username, password string) error {
	hash, err := scrypt.GenerateFromPassword([]byte(password), scrypt.DefaultParams)
	if err != nil {
		return err
	}
	userSql, userArgs, _ := s.PSQL.Insert("users").
		Columns("name", "username", "hashed_password", "is_admin").
		Values(username, username, string(hash), true).ToSql()
	_, err = s.DB.Exec(userSql, userArgs...)
	if err != nil {
		return err
	}
	return nil
}

func (s *CockroachStore) AuthenticateUserWithCredentials(username, password string) (*rcjpb.User, error) {
	countSql, countArgs, _ := s.PSQL.Select("count(*) as user_count").From("users").ToSql()
	userCount := struct {
		UserCount int `db:"user_count"`
	}{}
	err := s.DB.Get(&userCount, countSql, countArgs...)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error counting users: %+v", err))
	}
	if userCount.UserCount == 0 {
		if err = s.createInitialUser(username, password); err != nil {
			return nil, errors.New(fmt.Sprintf("Error creating initial user: %+v", err))
		}
	}
	sql, args, _ := s.PSQL.Select("id", "name", "username", "hashed_password", "is_admin").
		From("users").Where(sq.Eq{"username": username}).ToSql()
	user := struct {
		ID       string `db:"id"`
		Name     string `db:"name"`
		Username string `db:"username"`
		IsAdmin  bool   `db:"is_admin"`
		Password string `db:"hashed_password"`
	}{}
	err = s.DB.Get(&user, sql, args...)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error fetching user: %+v", err))
	}
	if user.ID == "" {
		return nil, errors.New("Error fetching user: Not found")
	}
	err = scrypt.CompareHashAndPassword([]byte(user.Password), []byte(password))
	if err != nil {
		return nil, nil
	}
	return &rcjpb.User{
		Id:       user.ID,
		Name:     user.Name,
		Username: user.Username,
		IsAdmin:  user.IsAdmin,
	}, nil
}

func (s *CockroachStore) FetchUser(id string, txx *sqlx.Tx) (*rcjpb.User, error) {
	sql, args, _ := s.PSQL.Select("id", "name", "username", "is_admin").
		From("users").Where(sq.Eq{"id": id}).ToSql()
	user := struct {
		ID       string `db:"id"`
		Name     string `db:"name"`
		Username string `db:"username"`
		IsAdmin  bool   `db:"is_admin"`
	}{}
	var err error
	if txx != nil {
		err = txx.Get(&user, sql, args...)
	} else {
		err = s.DB.Get(&user, sql, args...)
	}
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error fetching user: %+v", err))
	}
	if user.ID == "" {
		return nil, errors.New("Error fetching user: Not found")
	}
	return &rcjpb.User{
		Id:       user.ID,
		Name:     user.Name,
		Username: user.Username,
		IsAdmin:  user.IsAdmin,
	}, nil
}

func (s *CockroachStore) FetchDanceLadders(showAll bool) ([]*rcjpb.DivisionLadder, error) {
	sql, args, _ := s.PSQL.Select(
		"teams.id as id",
		"teams.name as name",
		"institutions.id as institution_id",
		"institutions.name as institution",
		"divisions.id as division_id",
		"divisions.name as division",
		"divisions.competition_rounds as competition_rounds",
		"divisions.final_rounds as final_rounds").From("teams").
		Join("institutions ON teams.institution = institutions.id").
		Join("divisions ON teams.division = divisions.id").
		Where(sq.Eq{"divisions.league": "On Stage"}).ToSql()
	type ladderTeam struct {
		ID                string `db:"id"`
		Name              string `db:"name"`
		Institution       string `db:"institution"`
		InstitutionID     string `db:"institution_id"`
		DivisionID        string `db:"division_id"`
		Division          string `db:"division"`
		CompetitionRounds int    `db:"competition_rounds"`
		FinalRounds       int    `db:"final_rounds"`
	}
	list := []ladderTeam{}
	err := s.DB.Select(&list, sql, args...)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error fetching teams: %+v", err))
	}
	innerScoreSql, _, _ := s.PSQL.Select(
		"score_sheets.id as id",
		"SUM(score_sheet_sections.value * score_sheet_template_sections.multiplier) as total",
	).From("score_sheets").
		Join("score_sheet_sections ON score_sheet_sections.score_sheet = score_sheets.id").
		Join("score_sheet_template_sections ON score_sheet_sections.section = score_sheet_template_sections.id").
		GroupBy("score_sheets.id").ToSql()
	scoreSql, scoreArgs, _ := s.PSQL.Select(
		"score_sheets.team as team",
		"score_sheets.round as round",
		"AVG(t1.total) as total",
		"count(*) as count",
	).From(fmt.Sprintf("(%s) as t1", innerScoreSql)).Join("score_sheets ON score_sheets.id = t1.id").
		GroupBy("score_sheets.team", "score_sheets.round").ToSql()
	type scoreCalc struct {
		Team    string          `db:"team"`
		Round   int             `db:"round"`
		Average decimal.Decimal `db:"total"`
		Count   int             `db:"count"`
	}
	scores := []scoreCalc{}
	err = s.DB.Select(&scores, scoreSql, scoreArgs...)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error fetching scores: %+v", err))
	}
	scoreMap := map[string][]*rcjpb.DivisionLadder_LadderEntry_RoundAverage{}
	for _, score := range scores {
		if _, ok := scoreMap[score.Team]; !ok {
			scoreMap[score.Team] = []*rcjpb.DivisionLadder_LadderEntry_RoundAverage{}
		}
		f, _ := score.Average.Round(2).Float64()
		scoreMap[score.Team] = append(scoreMap[score.Team], &rcjpb.DivisionLadder_LadderEntry_RoundAverage{
			Round:   int32(score.Round),
			Average: f,
			Count:   int32(score.Count),
		})
	}
	ladderMap := map[string]*rcjpb.DivisionLadder{}
	for _, team := range list {
		if _, ok := ladderMap[team.DivisionID]; !ok {
			ladderMap[team.DivisionID] = &rcjpb.DivisionLadder{
				Division: &rcjpb.Division{
					Id:                team.DivisionID,
					Name:              team.Division,
					League:            rcjpb.Division_ONSTAGE,
					CompetitionRounds: int32(team.CompetitionRounds),
					FinalRounds:       int32(team.FinalRounds),
				},
				Ladder: []*rcjpb.DivisionLadder_LadderEntry{},
			}
		}
		ladder := ladderMap[team.DivisionID]
		interviewScore := decimal.Decimal{}
		bestRound := decimal.Decimal{}
		bestFinal := decimal.Decimal{}
		pTeam := &rcjpb.Team{
			Id:   team.ID,
			Name: team.Name,
			Institution: &rcjpb.Institution{
				Id:   team.InstitutionID,
				Name: team.Institution,
			},
			Division: team.DivisionID,
		}
		ladderEntry := &rcjpb.DivisionLadder_LadderEntry{
			Team:   pTeam,
			Rounds: []*rcjpb.DivisionLadder_LadderEntry_RoundAverage{},
		}
		if scoreList, ok := scoreMap[team.ID]; ok {
			ladderEntry.Rounds = scoreList
			for _, score := range scoreList {
				dec := decimal.NewFromFloat(score.Average)
				if score.Round == 0 {
					interviewScore = interviewScore.Add(dec)
				} else if score.Round <= int32(team.CompetitionRounds) {
					if dec.GreaterThan(bestRound) {
						bestRound = dec
					}
				} else if score.Round > int32(team.CompetitionRounds) {
					if dec.GreaterThan(bestFinal) {
						bestFinal = dec
					}
				}
			}
		}
		interviewFloat, _ := interviewScore.Float64()
		ladderEntry.InterviewScore = interviewFloat
		bestRoundFloat, _ := bestRound.Float64()
		ladderEntry.BestRound = bestRoundFloat
		bestFinalFloat, _ := bestFinal.Float64()
		ladderEntry.BestFinal = bestFinalFloat
		roundTotalFloat, _ := interviewScore.Add(bestRound).Float64()
		ladderEntry.RoundTotal = roundTotalFloat
		if ladderEntry.BestFinal > 0.0 {
			finalTotalFloat, _ := interviewScore.Add(bestFinal).Float64()
			ladderEntry.FinalTotal = finalTotalFloat
		} else {
			ladderEntry.FinalTotal = 0.0
		}
		ladder.Ladder = append(ladder.Ladder, ladderEntry)
	}
	results := []*rcjpb.DivisionLadder{}
	for _, value := range ladderMap {
		sort.SliceStable(value.Ladder, func(i, j int) bool {
			if value.Ladder[i].FinalTotal < value.Ladder[j].FinalTotal {
				return true
			}
			if value.Ladder[i].FinalTotal > value.Ladder[j].FinalTotal {
				return false
			}
			if value.Ladder[i].RoundTotal < value.Ladder[j].RoundTotal {
				return true
			}
			if value.Ladder[i].RoundTotal > value.Ladder[j].RoundTotal {
				return false
			}
			return value.Ladder[i].Team.Name < value.Ladder[j].Team.Name
		})
		results = append(results, value)
	}
	return results, nil
}

func (s *CockroachStore) FetchDivision(id string) (*rcjpb.Division, error) {
	sql, args, _ := s.PSQL.Select(
		"id",
		"name",
		"league",
		"competition_rounds",
		"final_rounds",
		"interview_template",
		"performance_template",
	).From("divisions").Where(sq.Eq{"id": id}).ToSql()
	division := struct {
		ID                  string  `db:"id"`
		Name                string  `db:"name"`
		League              string  `db:"league"`
		CompetitionRounds   int     `db:"competition_rounds"`
		FinalRounds         int     `db:"final_rounds"`
		InterviewTemplate   *string `db:"interview_template"`
		PerformanceTemplate *string `db:"performance_template"`
	}{}
	err := s.DB.Get(&division, sql, args...)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error fetching division: %+v", err))
	}
	if division.ID == "" {
		return nil, nil
	}
	league := rcjpb.Division_ONSTAGE
	if division.League == "Soccer" {
		league = rcjpb.Division_SOCCER
	} else if division.League == "Rescue" {
		league = rcjpb.Division_RESCUE
	}
	returnDiv := &rcjpb.Division{
		Id:                division.ID,
		Name:              division.Name,
		League:            league,
		CompetitionRounds: int32(division.CompetitionRounds),
		FinalRounds:       int32(division.FinalRounds),
	}
	if division.InterviewTemplate != nil {
		returnDiv.InterviewTemplateId = *division.InterviewTemplate
	}
	if division.PerformanceTemplate != nil {
		returnDiv.PerformanceTemplateId = *division.PerformanceTemplate
	}
	return returnDiv, nil
}

type FetchTeamsOptions struct {
	PopulateMembers bool
	Division        *string
	ImportID        []string
}

func (s *CockroachStore) FetchTeams(ctx context.Context, options *FetchTeamsOptions) ([]*rcjpb.Team, error) {
	query := s.PSQL.Select(
		"teams.id as id",
		"teams.name as name",
		"institutions.id as institution_id",
		"institutions.name as institution",
		"teams.import_id as import_id",
		"teams.division as division",
	).From("teams").
		Join("institutions ON teams.institution = institutions.id")
	if options != nil {
		if options.Division != nil {
			query = query.Where(sq.Eq{"teams.division": options.Division})
		}
		if len(options.ImportID) > 0 {
			query = query.Where(sq.Eq{"teams.import_id": options.ImportID})
		}
	}
	sql, args, _ := query.ToSql()
	teams := []struct {
		ID            string `db:"id"`
		Name          string `db:"name"`
		InstitutionID string `db:"institution_id"`
		Institution   string `db:"institution"`
		ImportID      string `db:"import_id"`
		Division      string `db:"division"`
	}{}
	err := s.DB.SelectContext(ctx, &teams, sql, args...)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error fetching teams: %+v", err))
	}
	results := make([]*rcjpb.Team, len(teams))
	for idx, dbTeam := range teams {
		results[idx] = &rcjpb.Team{
			Id:   dbTeam.ID,
			Name: dbTeam.Name,
			Institution: &rcjpb.Institution{
				Id:   dbTeam.InstitutionID,
				Name: dbTeam.Institution,
			},
			ImportId: dbTeam.ImportID,
			Division: dbTeam.Division,
			Members:  []*rcjpb.Member{},
		}
	}
	if options != nil && !options.PopulateMembers {
		return results, nil
	}
	memberQuery := s.PSQL.Select(
		"team_members.id as id",
		"team_members.name as name",
		"team_members.gender as gender",
		"teams.division as division",
		"teams.id as team",
	).From("team_members").Join("teams ON teams.id = team_members.team")
	if options != nil {
		if options.Division != nil {
			memberQuery = memberQuery.Where(sq.Eq{"teams.division": options.Division})
		}
	}
	mSql, mArgs, _ := memberQuery.ToSql()
	type dbMember struct {
		ID       string `db:"id"`
		Name     string `db:"name"`
		Gender   string `db:"gender"`
		Division string `db:"division"`
		Team     string `db:"team"`
	}
	dbMemberObj := []dbMember{}
	err = s.DB.SelectContext(ctx, &dbMemberObj, mSql, mArgs...)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error fetching members: %+v", err))
	}
	for _, dbMember := range dbMemberObj {
		member := &rcjpb.Member{
			Id:     dbMember.ID,
			Name:   dbMember.Name,
			Gender: rcjpb.Member_UNSPECIFIED,
		}
		if dbMember.Gender == "Male" {
			member.Gender = rcjpb.Member_MALE
		} else if dbMember.Gender == "Female" {
			member.Gender = rcjpb.Member_FEMALE
		}
		for _, resultTeam := range results {
			if resultTeam.GetId() == dbMember.Team {
				resultTeam.Members = append(resultTeam.Members, member)
				break
			}
		}
	}
	return results, nil
}

type FetchScoreSheetTemplateOptions struct {
	IDs []string
}

func (s *CockroachStore) FetchScoreSheetTemplates(ctx context.Context, options *FetchScoreSheetTemplateOptions) ([]*rcjpb.ScoreSheetTemplate, error) {
	query := s.PSQL.Select(
		"id",
		"name",
		"type",
		"timings",
	).From("score_sheet_templates")
	sectionQuery := s.PSQL.Select(
		"id",
		"title",
		"description",
		"max_value",
		"multiplier",
		"score_sheet_template",
	).From("score_sheet_template_sections").OrderBy("display_order")
	if options != nil {
		if len(options.IDs) > 0 {
			query = query.Where(sq.Eq{"id": options.IDs})
			sectionQuery = sectionQuery.Where(sq.Eq{"score_sheet_template": options.IDs})
		}
	}
	group, _ := errgroup.WithContext(ctx)
	type dbTemplate struct {
		ID           string `db:"id"`
		Name         string `db:"name"`
		Type         string `db:"type"`
		TimingString string `db:"timings"`
	}
	type dbSection struct {
		ID                 string `db:"id"`
		Title              string `db:"title"`
		Description        string `db:"description"`
		MaxValue           int    `db:"max_value"`
		Multiplier         int    `db:"multiplier"`
		ScoreSheetTemplate string `db:"score_sheet_template"`
	}
	dbTemplates := []*dbTemplate{}
	dbSections := []*dbSection{}
	var lock sync.Mutex
	group.Go(func() error {
		sql, args, _ := query.ToSql()
		innerTemplates := []*dbTemplate{}
		err := s.DB.Select(&innerTemplates, sql, args...)
		if err != nil {
			return err
		}
		defer lock.Unlock()
		lock.Lock()
		dbTemplates = innerTemplates
		return nil
	})
	group.Go(func() error {
		sql, args, _ := sectionQuery.ToSql()
		innerSections := []*dbSection{}
		err := s.DB.Select(&innerSections, sql, args...)
		if err != nil {
			return err
		}
		defer lock.Unlock()
		lock.Lock()
		dbSections = innerSections
		return nil
	})
	err := group.Wait()
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error fetching: %+v", err))
	}
	sectionMap := map[string][]*rcjpb.ScoreSheetTemplateSection{}
	for _, dbSection := range dbSections {
		if _, ok := sectionMap[dbSection.ScoreSheetTemplate]; !ok {
			sectionMap[dbSection.ScoreSheetTemplate] = []*rcjpb.ScoreSheetTemplateSection{}
		}
		section := &rcjpb.ScoreSheetTemplateSection{
			Id:          dbSection.ID,
			Title:       dbSection.Title,
			Description: dbSection.Description,
			MaxValue:    int32(dbSection.MaxValue),
			Multiplier:  int32(dbSection.Multiplier),
		}
		sectionMap[dbSection.ScoreSheetTemplate] = append(sectionMap[dbSection.ScoreSheetTemplate], section)
	}
	templates := make([]*rcjpb.ScoreSheetTemplate, len(dbTemplates))
	for idx, dbTemplate := range dbTemplates {
		template := &rcjpb.ScoreSheetTemplate{
			Id:   dbTemplate.ID,
			Name: dbTemplate.Name,
		}
		if strings.ToLower(dbTemplate.Type) == "interview" {
			template.Type = rcjpb.ScoreSheetTemplate_INTERVIEW
		} else {
			template.Type = rcjpb.ScoreSheetTemplate_PERFORMANCE
		}
		cleanTimingString := dbTemplate.TimingString[1 : len(dbTemplate.TimingString)-1]
		if len(cleanTimingString) > 0 {
			for _, str := range strings.Split(cleanTimingString, ",") {
				template.Timings = append(template.Timings, str[1:len(str)-1])
			}
		}
		if sections, ok := sectionMap[dbTemplate.ID]; ok {
			template.Sections = sections
		}
		templates[idx] = template
	}
	return templates, nil
}

func (s *CockroachStore) FetchScoreSheet(ctx context.Context, id string, txx *sqlx.Tx) (*rcjpb.ScoreSheet, error) {
	sheetQuery := s.PSQL.Select(
		"score_sheets.id as id",
		"score_sheets.template as template",
		"score_sheet_templates.type as type",
		"score_sheets.comments as comments",
		"score_sheets.timings as timings",
		"score_sheets.team as team_id",
		"score_sheets.division as division",
		"teams.name as team",
		"score_sheets.round as round",
		"institutions.id as team_institution_id",
		"institutions.name as team_institution_name",
		"users.id as author",
		"users.name as author_name",
		"users.username as author_username",
	).From("score_sheets").
		Join("score_sheet_templates ON score_sheets.template = score_sheet_templates.id").
		Join("teams ON score_sheets.team = teams.id").
		Join("institutions ON teams.institution = institutions.id").
		Join("users ON users.id = score_sheets.author").Where(sq.Eq{"score_sheets.id": id})
	sectionQuery := s.PSQL.Select(
		"score_sheet_sections.id as id",
		"score_sheet_template_sections.title as title",
		"score_sheet_template_sections.description as description",
		"score_sheet_template_sections.max_value as max_value",
		"score_sheet_template_sections.multiplier as multiplier",
		"score_sheet_sections.section as section",
		"score_sheet_sections.value as value",
	).From("score_sheet_sections").
		Join("score_sheet_template_sections ON score_sheet_template_sections.id = score_sheet_sections.section").
		Where(sq.Eq{"score_sheet": id}).OrderBy("score_sheet_template_sections.display_order")

	type dbScoreSheet struct {
		ID              string         `db:"id"`
		TemplateID      string         `db:"template"`
		Type            string         `db:"type"`
		Comments        string         `db:"comments"`
		Timings         types.JSONText `db:"timings"`
		TeamID          string         `db:"team_id"`
		Division        string         `db:"division"`
		Team            string         `db:"team"`
		Round           int            `db:"round"`
		InstitutionID   string         `db:"team_institution_id"`
		InstitutionName string         `db:"team_institution_name"`
		AuthorID        string         `db:"author"`
		AuthorName      string         `db:"author_name"`
		AuthorUsername  string         `db:"author_username"`
	}
	type dbScoreSheetSection struct {
		ID          string  `db:"id"`
		Title       string  `db:"title"`
		Description string  `db:"description"`
		MaxValue    int     `db:"max_value"`
		Multiplier  int     `db:"multiplier"`
		SectionID   string  `db:"section"`
		Value       float32 `db:"value"`
	}
	var scoreSheet *dbScoreSheet
	sections := []*dbScoreSheetSection{}
	var lock sync.Mutex

	group, _ := errgroup.WithContext(ctx)
	group.Go(func() error {
		sql, args, _ := sheetQuery.ToSql()
		fetchedSheet := dbScoreSheet{}
		var err error
		if txx != nil {
			err = txx.Get(&fetchedSheet, sql, args...)
		} else {
			err = s.DB.Get(&fetchedSheet, sql, args...)
		}
		if err != nil {
			return err
		}
		defer lock.Unlock()
		lock.Lock()
		scoreSheet = &fetchedSheet
		return nil
	})
	group.Go(func() error {
		sql, args, _ := sectionQuery.ToSql()
		fetchedSections := []*dbScoreSheetSection{}
		var err error
		if txx != nil {
			err = txx.Select(&fetchedSections, sql, args...)
		} else {
			err = s.DB.Select(&fetchedSections, sql, args...)
		}
		if err != nil {
			return err
		}
		defer lock.Unlock()
		lock.Lock()
		sections = fetchedSections
		return nil
	})
	err := group.Wait()
	if err != nil {
		fmt.Printf("ERR %+v\n", err)
		return nil, errors.New(fmt.Sprintf("Error fetching: %+v", err))
	}
	if scoreSheet == nil {
		return nil, nil
	}
	timings := []struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}{}
	scoreSheet.Timings.Unmarshal(&timings)
	pbTimings := make([]*rcjpb.ScoreSheet_Timing, len(timings))
	for idx, timing := range timings {
		pbTimings[idx] = &rcjpb.ScoreSheet_Timing{
			Name:  timing.Name,
			Value: timing.Value,
		}
	}
	score := &rcjpb.ScoreSheet{
		Id:                   scoreSheet.ID,
		ScoreSheetTemplateId: scoreSheet.TemplateID,
		Comments:             scoreSheet.Comments,
		Round:                int32(scoreSheet.Round),
		Team: &rcjpb.Team{
			Id:   scoreSheet.TeamID,
			Name: scoreSheet.Team,
			Institution: &rcjpb.Institution{
				Id:   scoreSheet.InstitutionID,
				Name: scoreSheet.InstitutionName,
			},
			Division: scoreSheet.Division,
		},
		Author: &rcjpb.User{
			Id:       scoreSheet.AuthorID,
			Name:     scoreSheet.AuthorName,
			Username: scoreSheet.AuthorUsername,
		},
		Sections:   make([]*rcjpb.ScoreSheetSection, len(sections)),
		Timings:    pbTimings,
		DivisionId: scoreSheet.Division,
	}
	if scoreSheet.Type == "Interview" {
		score.Type = rcjpb.ScoreSheetTemplate_INTERVIEW
	} else {
		score.Type = rcjpb.ScoreSheetTemplate_PERFORMANCE
	}
	for idx, section := range sections {
		pbSection := &rcjpb.ScoreSheetSection{
			Id:          section.ID,
			Title:       section.Title,
			Description: section.Description,
			MaxValue:    int32(section.MaxValue),
			Multiplier:  int32(section.Multiplier),
			SectionId:   section.SectionID,
			Value:       float64(section.Value),
		}
		score.Sections[idx] = pbSection
	}
	return score, nil
}

func (s *CockroachStore) CreateScoreSheet(ctx context.Context, handler func(*rcjpb.ScoreSheet) error) (*rcjpb.ScoreSheet, error) {
	var scoreSheetID string
	err := crdb.ExecuteTxx(ctx, s.DB, nil, func(tx *sqlx.Tx) error {
		scoreSheet := &rcjpb.ScoreSheet{}
		handlerError := handler(scoreSheet)
		if handlerError != nil {
			return handlerError
		}

		type scoreSheetTiming struct {
			Name  string `json:"name"`
			Value string `json:"value"`
		}
		jsonTimings := make([]scoreSheetTiming, len(scoreSheet.GetTimings()))
		for idx, timing := range scoreSheet.GetTimings() {
			jsonTimings[idx] = scoreSheetTiming{
				Name:  timing.GetName(),
				Value: timing.GetValue(),
			}
		}
		b, err := json.Marshal(jsonTimings)
		if err != nil {
			return err
		}

		ssSql, ssArgs, _ := s.PSQL.Insert("score_sheets").
			Columns("division", "team", "template", "timings", "comments", "round", "author").
			Values(
				scoreSheet.GetDivisionId(),
				scoreSheet.GetTeam().GetId(),
				scoreSheet.GetScoreSheetTemplateId(),
				string(b),
				scoreSheet.GetComments(),
				scoreSheet.GetRound(),
				scoreSheet.GetAuthor().GetId(),
			).Suffix("RETURNING \"id\"").ToSql()

		ssRows, err := tx.Query(ssSql, ssArgs...)
		if err != nil {
			return err
		}
		for ssRows.Next() {
			ssRows.Scan(&scoreSheetID)
		}
		ssRows.Close()

		secQuery := s.PSQL.Insert("score_sheet_sections").
			Columns("section", "value", "score_sheet")
		for _, section := range scoreSheet.GetSections() {
			secQuery = secQuery.Values(section.GetSectionId(), section.GetValue(), scoreSheetID)
		}
		secSql, secArgs, _ := secQuery.ToSql()
		_, err = tx.Exec(secSql, secArgs...)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s.FetchScoreSheet(ctx, scoreSheetID, nil)
}

func (s *CockroachStore) UpdateScoreSheet(ctx context.Context, scoreSheetId string, handler func(*rcjpb.ScoreSheet) error) (*rcjpb.ScoreSheet, error) {
	err := crdb.ExecuteTxx(ctx, s.DB, nil, func(tx *sqlx.Tx) error {
		scoreSheet, err := s.FetchScoreSheet(ctx, scoreSheetId, tx)
		if err != nil {
			return err
		}
		handlerError := handler(scoreSheet)
		if handlerError != nil {
			return handlerError
		}
		type scoreSheetTiming struct {
			Name  string `json:"name"`
			Value string `json:"value"`
		}
		jsonTimings := make([]scoreSheetTiming, len(scoreSheet.GetTimings()))
		for idx, timing := range scoreSheet.GetTimings() {
			jsonTimings[idx] = scoreSheetTiming{
				Name:  timing.GetName(),
				Value: timing.GetValue(),
			}
		}
		b, err := json.Marshal(jsonTimings)
		if err != nil {
			return err
		}
		ssUpdateFields := map[string]interface{}{
			"team":     scoreSheet.Team.GetId(),
			"timings":  string(b),
			"comments": scoreSheet.GetComments(),
		}
		ssSql, ssArgs, _ := s.PSQL.Update("score_sheets").SetMap(ssUpdateFields).Where(sq.Eq{"id": scoreSheetId}).ToSql()
		_, err = tx.Exec(ssSql, ssArgs...)
		if err != nil {
			return err
		}
		for _, section := range scoreSheet.GetSections() {
			sectSql, sectArgs, _ := s.PSQL.Update("score_sheet_sections").Set("value", section.GetValue()).Where(sq.Eq{"id": section.GetId()}).ToSql()
			_, err = tx.Exec(sectSql, sectArgs...)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s.FetchScoreSheet(ctx, scoreSheetId, nil)
}

type FetchScoreSheetSummaryOptions struct {
	TeamID       *string
	AuthorID     *string
	PopulateTeam bool
}

func (s *CockroachStore) FetchScoreSheetSummary(ctx context.Context, opts *FetchScoreSheetSummaryOptions, txx *sqlx.Tx) ([]*rcjpb.ScoreSheet, error) {
	innerQuery := s.PSQL.Select(
		"score_sheets.id as id",
		"SUM(score_sheet_sections.value * score_sheet_template_sections.multiplier) as total",
	).From("score_sheets").
		Join("score_sheet_sections ON score_sheet_sections.score_sheet = score_sheets.id").
		Join("score_sheet_template_sections ON score_sheet_sections.section = score_sheet_template_sections.id")
	if opts != nil {
		if opts.TeamID != nil {
			innerQuery = innerQuery.Where(sq.Eq{"score_sheets.team": opts.TeamID})
		}
		if opts.AuthorID != nil {
			innerQuery = innerQuery.Where(sq.Eq{"score_sheets.author": opts.AuthorID})
		}
	}
	innerSql, innerArgs, _ := innerQuery.GroupBy("score_sheets.id").ToSql()
	ssSql, _, _ := s.PSQL.
		Select(
			"score_sheets.id as id",
			"divisions.id as division_id",
			"divisions.name as division",
			"score_sheet_templates.name as template",
			"score_sheets.round as round",
			"score_sheet_templates.type as type",
			"t1.total as total",
			"users.id as author_id",
			"users.name as author",
			"teams.id as team_id",
			"teams.name as team_name",
			"institutions.id as institution_id",
			"institutions.name as institution_name",
		).
		From(fmt.Sprintf("(%s) as t1", innerSql)).
		Join("score_sheets ON score_sheets.id = t1.id").
		Join("score_sheet_templates ON score_sheets.template = score_sheet_templates.id").
		Join("divisions ON score_sheets.division = divisions.id").
		Join("users ON users.id = score_sheets.author").
		Join("teams ON teams.id = score_sheets.team").
		Join("institutions ON teams.institution = institutions.id").
		OrderBy("round ASC").ToSql()
	type scoreSheetListEntry struct {
		ID              string          `db:"id"`
		Division        string          `db:"division"`
		DivisionID      string          `db:"division_id"`
		Template        string          `db:"template"`
		Type            string          `db:"type"`
		Round           int             `db:"round"`
		Total           decimal.Decimal `db:"total"`
		Author          string          `db:"author"`
		AuthorID        string          `db:"author_id"`
		TeamID          string          `db:"team_id"`
		TeamName        string          `db:"team_name"`
		InstitutionID   string          `db:"institution_id"`
		InstitutionName string          `db:"institution_name"`
	}
	list := []*scoreSheetListEntry{}
	err := s.DB.Select(&list, ssSql, innerArgs...)
	if err != nil {
		return nil, err
	}
	scoreSheets := []*rcjpb.ScoreSheet{}
	for _, entry := range list {
		fl, _ := entry.Total.Float64()
		scoreSheet := &rcjpb.ScoreSheet{
			Id:         entry.ID,
			Round:      int32(entry.Round),
			DivisionId: entry.DivisionID,
			Total:      fl,
			Author: &rcjpb.User{
				Id:   entry.AuthorID,
				Name: entry.Author,
			},
			Team: &rcjpb.Team{
				Id:   entry.TeamID,
				Name: entry.TeamName,
				Institution: &rcjpb.Institution{
					Id:   entry.InstitutionID,
					Name: entry.InstitutionName,
				},
			},
		}
		if entry.Type == "Interview" {
			scoreSheet.Type = rcjpb.ScoreSheetTemplate_INTERVIEW
		} else {
			scoreSheet.Type = rcjpb.ScoreSheetTemplate_PERFORMANCE
		}
		scoreSheets = append(scoreSheets, scoreSheet)
	}
	return scoreSheets, nil
}

func (s *CockroachStore) FetchTeam(ctx context.Context, id string, txx *sqlx.Tx) (*rcjpb.Team, error) {
	sql, args, _ := s.PSQL.Select(
		"teams.id as id",
		"teams.name as name",
		"teams.institution as institution_id",
		"institutions.name as institution",
		"teams.division as division_id",
	).From("teams").
		Join("institutions ON teams.institution = institutions.id").
		Where(sq.Eq{"teams.id": id}).ToSql()
	memSql, memArgs, _ := s.PSQL.Select("id", "name", "gender").
		From("team_members").Where(sq.Eq{"team": id}).ToSql()
	type dbTeam struct {
		ID            string `db:"id"`
		Name          string `db:"name"`
		InstitutionID string `db:"institution_id" json:"institution_id"`
		Institution   string `db:"institution" json:"institution"`
		DivisionID    string `db:"division_id" json:"division_id"`
	}
	dbObj := dbTeam{}
	type dbMember struct {
		ID     string `db:"id"`
		Name   string `db:"name"`
		Gender string `db:"gender"`
	}
	dbMemberObj := []dbMember{}
	group, _ := errgroup.WithContext(ctx)
	group.Go(func() error {
		var err error
		if txx != nil {
			err = txx.Get(&dbObj, sql, args...)
		} else {
			err = s.DB.Get(&dbObj, sql, args...)
		}
		return err
	})
	group.Go(func() error {
		var err error
		if txx != nil {
			err = txx.Select(&dbMemberObj, memSql, memArgs...)
		} else {
			err = s.DB.Select(&dbMemberObj, memSql, memArgs...)
		}
		return err
	})
	err := group.Wait()
	if err != nil {
		return nil, err
	}
	team := &rcjpb.Team{
		Id:   dbObj.ID,
		Name: dbObj.Name,
		Institution: &rcjpb.Institution{
			Id:   dbObj.InstitutionID,
			Name: dbObj.Institution,
		},
		Division: dbObj.DivisionID,
		Members:  []*rcjpb.Member{},
	}
	for _, dbMember := range dbMemberObj {
		member := &rcjpb.Member{
			Id:     dbMember.ID,
			Name:   dbMember.Name,
			Gender: rcjpb.Member_UNSPECIFIED,
		}
		if dbMember.Gender == "Male" {
			member.Gender = rcjpb.Member_MALE
		} else if dbMember.Gender == "Female" {
			member.Gender = rcjpb.Member_FEMALE
		}
		team.Members = append(team.Members, member)
	}
	return team, nil
}

func (s *CockroachStore) FetchDivisions(ctx context.Context, txx *sqlx.Tx) ([]*rcjpb.Division, error) {
	sql, args, _ := s.PSQL.Select("id", "name", "league").From("divisions").ToSql()
	type division struct {
		ID     string `db:"id"`
		Name   string `db:"name"`
		League string `db:"league"`
	}
	var err error
	divs := []division{}
	if txx == nil {
		err = s.DB.Select(&divs, sql, args...)
	} else {
		err = txx.Select(&divs, sql, args...)
	}
	if err != nil {
		return nil, err
	}
	protoDivs := []*rcjpb.Division{}
	for _, entry := range divs {
		protoDiv := &rcjpb.Division{
			Id:     entry.ID,
			Name:   entry.Name,
			League: rcjpb.Division_ONSTAGE,
		}
		if entry.League == "Rescue" {
			protoDiv.League = rcjpb.Division_RESCUE
		} else if entry.League == "Soccer" {
			protoDiv.League = rcjpb.Division_SOCCER
		}
		protoDivs = append(protoDivs, protoDiv)
	}
	return protoDivs, nil
}

func (s *CockroachStore) innerCreateTeam(txx *sqlx.Tx, team *rcjpb.Team) (string, error) {
	institutionID := ""
	if team.Institution.GetId() == "" {
		instSql, instArgs, _ := s.PSQL.Insert("institutions").Columns("name").Values(team.Institution.GetName()).Suffix("RETURNING \"id\"").ToSql()
		instRows, instErr := txx.Query(instSql, instArgs...)
		if instErr != nil {
			return "", instErr
		}
		defer instRows.Close()
		for instRows.Next() {
			instRows.Scan(&institutionID)
		}
	} else {
		institutionID = team.Institution.GetId()
	}
	teamSql, teamArgs, _ := s.PSQL.Insert("teams").
		Columns("name", "institution", "division", "import_id").
		Values(team.GetName(), institutionID, team.GetDivision(), team.GetImportId()).
		Suffix("RETURNING \"id\"").ToSql()
	teamRows, teamErr := txx.Query(teamSql, teamArgs...)
	if teamErr != nil {
		return "", teamErr
	}
	defer teamRows.Close()
	teamID := ""
	for teamRows.Next() {
		teamRows.Scan(&teamID)
	}
	memberQuery := s.PSQL.Insert("team_members").Columns("name", "gender", "team")
	for _, member := range team.Members {
		gender := "Not Specified"
		if member.GetGender() == rcjpb.Member_MALE {
			gender = "Male"
		} else if member.GetGender() == rcjpb.Member_FEMALE {
			gender = "Female"
		}
		memberQuery = memberQuery.Values(member.GetName(), gender, teamID)
	}
	mSql, mArgs, _ := memberQuery.Suffix("RETURNING \"id\"").ToSql()
	_, mErr := txx.Query(mSql, mArgs...)
	if mErr != nil {
		return "", mErr
	}
	return teamID, nil
}

func (s *CockroachStore) CreateTeam(ctx context.Context, txx *sqlx.Tx, handler func(*rcjpb.Team) error) (*rcjpb.Team, error) {
	team := &rcjpb.Team{}
	handlerErr := handler(team)
	if handlerErr != nil {
		return nil, handlerErr
	}
	teamID := ""
	if txx == nil {
		err := crdb.ExecuteTxx(ctx, s.DB, nil, func(tx *sqlx.Tx) error {
			createdTeamID, teamErr := s.innerCreateTeam(tx, team)
			if teamErr != nil {
				return teamErr
			}
			teamID = createdTeamID
			return nil
		})
		if err != nil {
			return nil, err
		}
	} else {
		createdTeamID, err := s.innerCreateTeam(txx, team)
		if err != nil {
			return nil, err
		}
		teamID = createdTeamID
	}
	return s.FetchTeam(ctx, teamID, txx)
}

func (s *CockroachStore) FetchInstitutions(ctx context.Context, search string) ([]*rcjpb.Institution, error) {
	sql, args, _ := s.PSQL.Select("id", "name").From("institutions").ToSql()
	type institution struct {
		ID   string `db:"id"`
		Name string `db:"name"`
	}
	insts := []institution{}
	err := s.DB.Select(&insts, sql, args...)
	if err != nil {
		return nil, err
	}
	protoInsts := []*rcjpb.Institution{}
	for _, entry := range insts {
		protoInst := &rcjpb.Institution{
			Id:   entry.ID,
			Name: entry.Name,
		}
		protoInsts = append(protoInsts, protoInst)
	}
	return protoInsts, nil
}

func (s *CockroachStore) UpdateTeam(ctx context.Context, teamID string, handler func(*rcjpb.Team) error) (*rcjpb.Team, error) {
	err := crdb.ExecuteTxx(ctx, s.DB, nil, func(tx *sqlx.Tx) error {
		team, err := s.FetchTeam(ctx, teamID, tx)
		if err != nil {
			return err
		}
		originalMembers := team.GetMembers()
		handlerErr := handler(team)
		if handlerErr != nil {
			return handlerErr
		}
		institutionID := ""
		if team.Institution.GetId() == "" {
			instSql, instArgs, _ := s.PSQL.Insert("institutions").Columns("name").Values(team.Institution.GetName()).Suffix("RETURNING \"id\"").ToSql()
			instRows, instErr := tx.Query(instSql, instArgs...)
			if instErr != nil {
				return instErr
			}
			defer instRows.Close()
			for instRows.Next() {
				instRows.Scan(&institutionID)
			}
		} else {
			institutionID = team.Institution.GetId()
		}
		teamSql, teamArgs, _ := s.PSQL.Update("teams").SetMap(map[string]interface{}{
			"name":        team.Name,
			"institution": institutionID,
			"division":    team.Division,
		}).Where(sq.Eq{"id": teamID}).ToSql()
		_, teamErr := tx.Exec(teamSql, teamArgs...)
		if teamErr != nil {
			return teamErr
		}
		memberInserts := s.PSQL.Insert("team_members").Columns("name", "gender", "team")
		memberInsertCount := 0
		for _, member := range team.Members {
			gender := "Not Specified"
			if member.GetGender() == rcjpb.Member_MALE {
				gender = "Male"
			} else if member.GetGender() == rcjpb.Member_FEMALE {
				gender = "Female"
			}
			if member.GetId() == "" {
				memberInserts = memberInserts.Values(member.GetName(), gender, team.GetId())
				memberInsertCount += 1
			} else {
				mSql, mArgs, _ := s.PSQL.Update("team_members").SetMap(map[string]interface{}{
					"name":   member.GetName(),
					"gender": gender,
				}).Where(sq.Eq{"id": member.GetId()}).ToSql()
				_, execErr := tx.Exec(mSql, mArgs...)
				if execErr != nil {
					return execErr
				}
			}
		}
		if memberInsertCount > 0 {
			insertSql, insertArgs, _ := memberInserts.ToSql()
			_, execErr := tx.Exec(insertSql, insertArgs...)
			if execErr != nil {
				return execErr
			}
		}
		for _, existingMember := range originalMembers {
			found := false
			for _, newMember := range team.Members {
				if newMember.GetId() != "" && newMember.GetId() == existingMember.GetId() {
					found = true
				}
			}
			if found == false {
				mSql, mArgs, _ := s.PSQL.Delete("team_members").Where(sq.Eq{"id": existingMember.GetId()}).ToSql()
				_, execErr := tx.Exec(mSql, mArgs...)
				if execErr != nil {
					return execErr
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s.FetchTeam(ctx, teamID, nil)
}

func (s *CockroachStore) CreateDivision(ctx context.Context, handler func(*rcjpb.Division) error) (*rcjpb.Division, error) {
	var divisionID string
	err := crdb.ExecuteTxx(ctx, s.DB, nil, func(tx *sqlx.Tx) error {
		division := &rcjpb.Division{}
		handlerError := handler(division)
		if handlerError != nil {
			return handlerError
		}
		leagueStr := "On Stage"
		if division.GetLeague() == rcjpb.Division_RESCUE {
			leagueStr = "Rescue"
		} else if division.GetLeague() == rcjpb.Division_SOCCER {
			leagueStr = "Soccer"
		}
		sql, args, _ := s.PSQL.Insert("divisions").Columns(
			"name",
			"league",
			"competition_rounds",
			"final_rounds",
			"interview_template",
			"performance_template",
		).Values(
			division.GetName(),
			leagueStr,
			division.GetCompetitionRounds(),
			division.GetFinalRounds(),
			division.GetInterviewTemplateId(),
			division.GetPerformanceTemplateId(),
		).Suffix("RETURNING \"id\"").ToSql()
		rows, err := tx.Query(sql, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			rows.Scan(&divisionID)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s.FetchDivision(divisionID)
}

func (s *CockroachStore) CreateScoreSheetTemplate(ctx context.Context, handler func(*rcjpb.ScoreSheetTemplate) error) (*rcjpb.ScoreSheetTemplate, error) {
	var templateID string
	err := crdb.ExecuteTxx(ctx, s.DB, nil, func(tx *sqlx.Tx) error {
		template := &rcjpb.ScoreSheetTemplate{}
		handlerError := handler(template)
		if handlerError != nil {
			return handlerError
		}
		typeStr := "Interview"
		if template.Type == rcjpb.ScoreSheetTemplate_PERFORMANCE {
			typeStr = "Performance"
		}
		arrayItems := make([]string, len(template.GetTimings()))
		for idx, str := range template.GetTimings() {
			arrayItems[idx] = fmt.Sprintf("\"%s\"", str)
		}
		arrayStr := strings.Join(arrayItems, ",")
		tempSql, tempArgs, _ := s.PSQL.Insert("score_sheet_templates").Columns(
			"name",
			"type",
			"timings",
		).Values(
			template.GetName(),
			typeStr,
			fmt.Sprintf("{%s}", arrayStr),
		).Suffix("RETURNING \"id\"").ToSql()
		tempRows, tempErr := tx.Query(tempSql, tempArgs...)
		if tempErr != nil {
			return tempErr
		}
		defer tempRows.Close()
		for tempRows.Next() {
			tempRows.Scan(&templateID)
		}
		sectionQuery := s.PSQL.Insert("score_sheet_template_sections").Columns(
			"title",
			"score_sheet_template",
			"description",
			"max_value",
			"multiplier",
			"display_order",
		)
		for _, section := range template.GetSections() {
			sectionQuery = sectionQuery.Values(
				section.GetTitle(),
				templateID,
				section.GetDescription(),
				section.GetMaxValue(),
				section.GetMaxValue(),
				section.GetDisplayOrder(),
			)
		}
		sectionSql, sectionArgs, _ := sectionQuery.ToSql()
		_, sectionErr := tx.Exec(sectionSql, sectionArgs...)
		if sectionErr != nil {
			return sectionErr
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	templates, err := s.FetchScoreSheetTemplates(ctx, &FetchScoreSheetTemplateOptions{
		IDs: []string{templateID},
	})
	if err != nil {
		return nil, err
	}
	if len(templates) != 1 {
		return nil, errors.New(fmt.Sprintf("More than 1 template returned"))
	}
	return templates[0], nil
}

func (s *CockroachStore) FetchUsers(ctx context.Context) ([]*rcjpb.User, error) {
	sql, args, _ := s.PSQL.Select("id", "name", "username", "is_admin").From("users").ToSql()
	type dbUser struct {
		ID       string `db:"id"`
		Name     string `db:"name"`
		Username string `db:"username"`
		IsAdmin  bool   `db:"is_admin"`
	}
	dbUsers := []dbUser{}
	err := s.DB.Select(&dbUsers, sql, args...)
	if err != nil {
		return nil, err
	}
	protoUsers := []*rcjpb.User{}
	for _, entry := range dbUsers {
		protoUser := &rcjpb.User{
			Id:       entry.ID,
			Name:     entry.Name,
			Username: entry.Username,
			IsAdmin:  entry.IsAdmin,
		}
		protoUsers = append(protoUsers, protoUser)
	}
	return protoUsers, nil
}

func (s *CockroachStore) CreateUser(ctx context.Context, handler func(*rcjpb.User) error) (*rcjpb.User, error) {
	var userID string
	err := crdb.ExecuteTxx(ctx, s.DB, nil, func(tx *sqlx.Tx) error {
		user := &rcjpb.User{}
		handlerError := handler(user)
		if handlerError != nil {
			return handlerError
		}
		hash, err := scrypt.GenerateFromPassword([]byte(user.Password), scrypt.DefaultParams)
		if err != nil {
			return err
		}
		sql, args, _ := s.PSQL.Insert("users").Columns(
			"name",
			"username",
			"hashed_password",
			"is_admin",
		).Values(
			user.GetName(),
			user.GetUsername(),
			string(hash),
			user.GetIsAdmin(),
		).Suffix("RETURNING \"id\"").ToSql()
		userRows, err := tx.Query(sql, args...)
		if err != nil {
			return err
		}
		defer userRows.Close()
		for userRows.Next() {
			userRows.Scan(&userID)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s.FetchUser(userID, nil)
}

func (s *CockroachStore) UpdateUser(ctx context.Context, userID string, handler func(*rcjpb.User) error) (*rcjpb.User, error) {
	err := crdb.ExecuteTxx(ctx, s.DB, nil, func(tx *sqlx.Tx) error {
		user, err := s.FetchUser(userID, tx)
		if err != nil {
			return err
		}
		handlerError := handler(user)
		if handlerError != nil {
			return handlerError
		}
		updateMap := map[string]interface{}{
			"name":     user.GetName(),
			"username": user.GetUsername(),
			"is_admin": user.GetIsAdmin(),
		}
		if len(user.GetPassword()) > 0 {
			hash, err := scrypt.GenerateFromPassword([]byte(user.Password), scrypt.DefaultParams)
			if err != nil {
				return err
			}
			updateMap["hashed_password"] = string(hash)
		}
		sql, args, _ := s.PSQL.Update("users").SetMap(updateMap).Where(sq.Eq{"id": userID}).ToSql()
		_, err = tx.Exec(sql, args...)
		return err
	})
	if err != nil {
		return nil, err
	}
	return s.FetchUser(userID, nil)
}

func (s *CockroachStore) FetchCheckins(ctx context.Context) ([]*rcjpb.Checkin, error) {
	sql, args, _ := s.PSQL.Select(
		"id",
		"team",
		"agent",
		"comments",
		"in_time",
	).From("team_checkins").ToSql()
	type checkinEntry struct {
		ID       string     `db:"id"`
		Team     string     `db:"team"`
		Agent    string     `db:"agent"`
		Comments string     `db:"comments"`
		InTime   *time.Time `db:"in_time"`
	}
	entries := []checkinEntry{}
	err := s.DB.Select(&entries, sql, args...)
	if err != nil {
		return nil, err
	}
	results := make([]*rcjpb.Checkin, len(entries))
	for idx, entry := range entries {
		sec := int64(entry.InTime.Second())
		n := int32(entry.InTime.Nanosecond())
		checkin := &rcjpb.Checkin{
			Id: entry.ID,
			Team: &rcjpb.Team{
				Id: entry.Team,
			},
			Agent: &rcjpb.User{
				Id: entry.Agent,
			},
			Comments: entry.Comments,
			InTime: &tspb.Timestamp{
				Seconds: sec,
				Nanos:   n,
			},
		}
		results[idx] = checkin
	}
	return results, nil
}

func (s *CockroachStore) FetchCheckin(ctx context.Context, id string) (*rcjpb.Checkin, error) {
	sql, args, _ := s.PSQL.Select(
		"id",
		"team",
		"agent",
		"comments",
		"in_time",
	).From("team_checkins").Where(sq.Eq{"id": id}).ToSql()
	type checkinEntry struct {
		ID       string     `db:"id"`
		Team     string     `db:"team"`
		Agent    string     `db:"agent"`
		Comments string     `db:"comments"`
		InTime   *time.Time `db:"in_time"`
	}
	entry := checkinEntry{}
	err := s.DB.Get(&entry, sql, args...)
	if err != nil {
		return nil, err
	}
	sec := int64(entry.InTime.Second())
	n := int32(entry.InTime.Nanosecond())
	checkin := &rcjpb.Checkin{
		Id: entry.ID,
		Team: &rcjpb.Team{
			Id: entry.Team,
		},
		Agent: &rcjpb.User{
			Id: entry.Agent,
		},
		Comments: entry.Comments,
		InTime: &tspb.Timestamp{
			Seconds: sec,
			Nanos:   n,
		},
	}
	return checkin, nil
}

func (s *CockroachStore) CreateCheckin(ctx context.Context, handler func(*rcjpb.Checkin) error) (*rcjpb.Checkin, error) {
	checkin := &rcjpb.Checkin{}
	handlerErr := handler(checkin)
	if handlerErr != nil {
		return nil, handlerErr
	}
	sql, args, _ := s.PSQL.Insert("team_checkins").Columns(
		"team",
		"agent",
		"comments",
	).Values(checkin.GetTeam().GetId(), checkin.GetAgent().GetId(), checkin.GetComments()).Suffix("RETURNING \"id\"").ToSql()
	var checkinID string
	checkinRows, err := s.DB.Query(sql, args...)
	if err != nil {
		return nil, err
	}
	for checkinRows.Next() {
		checkinRows.Scan(&checkinID)
	}
	checkinRows.Close()
	return s.FetchCheckin(ctx, checkinID)
}
