package sheets

import (
	"context"
	"encoding/json"
	"fmt"
	sq "github.com/Masterminds/squirrel"
	rcjpb "github.com/davefinster/rcj-go/api/proto"
	"github.com/jmoiron/sqlx"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/sheets/v4"
	"io/ioutil"
	"net/http"
)

type SheetStore struct {
	SecretPath string
	DB         *sqlx.DB
	PSQL       sq.StatementBuilderType
	AuthConfig *oauth2.Config
}

func NewSheetStore(secretPath string) *SheetStore {
	return &SheetStore{
		SecretPath: secretPath,
		PSQL:       sq.StatementBuilder.PlaceholderFormat(sq.Dollar),
	}
}

func (s *SheetStore) getClient(c *oauth2.Config) (*http.Client, error) {
	tok := &oauth2.Token{}
	token, err := s.getTokenEntry("auth")
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal([]byte(token), &tok)
	if err != nil {
		return nil, err
	}
	return c.Client(context.Background(), tok), nil
}

func (s *SheetStore) getSheetsService() (*sheets.Service, error) {
	b, err := ioutil.ReadFile(s.SecretPath)
	if err != nil {
		return nil, err
	}
	config, err := google.ConfigFromJSON(b, "https://www.googleapis.com/auth/spreadsheets")
	if err != nil {
		return nil, err
	}
	client, err := s.getClient(config)
	if err != nil {
		return nil, err
	}
	return sheets.New(client)
}

func (s *SheetStore) setTokenEntry(token, tokenType string) error {
	delSql, delArgs, _ := s.PSQL.Delete("sheet_token").Where(sq.Eq{"token_type": tokenType}).ToSql()
	sql, args, err := s.PSQL.Insert("sheet_token").Columns("token", "token_type").Values(token, tokenType).ToSql()
	_, err = s.DB.Exec(delSql, delArgs...)
	if err != nil {
		return err
	}
	_, err = s.DB.Exec(sql, args...)
	return err
}

func (s *SheetStore) getTokenEntry(tokenType string) (string, error) {
	sql, args, _ := s.PSQL.Select("token").From("sheet_token").Where(sq.Eq{"token_type": tokenType}).Limit(1).ToSql()
	type tokenRow struct {
		Token string
	}
	tokens := []tokenRow{}
	err := s.DB.Select(&tokens, sql, args...)
	if err != nil {
		return "", err
	}
	if len(tokens) == 0 {
		return "", nil
	}
	return tokens[0].Token, nil
}

func (s *SheetStore) AuthURL() (string, error) {
	b, err := ioutil.ReadFile(s.SecretPath)
	if err != nil {
		return "", err
	}
	config, err := google.ConfigFromJSON(b, "https://www.googleapis.com/auth/spreadsheets")
	if err != nil {
		return "", err
	}
	s.AuthConfig = config
	return config.AuthCodeURL("state-token", oauth2.AccessTypeOffline, oauth2.ApprovalForce), nil
}

// SpreadsheetConfig - The IDs of supported spreadsheets for Google Sheet sync
type SpreadsheetConfig struct {
	IngestionSpreadsheetID string
	PushSpreadsheetID      string
}

func (s *SheetStore) SetIngestionSpreadsheet(id string) error {
	return s.setTokenEntry(id, "ingestion")
}

func (s *SheetStore) SetPushSpreadsheet(id string) error {
	return s.setTokenEntry(id, "push")
}

func (s *SheetStore) GetSpreadsheets() (*SpreadsheetConfig, error) {
	ingest, err := s.getTokenEntry("ingestion")
	if err != nil {
		return nil, err
	}
	push, err := s.getTokenEntry("push")
	if err != nil {
		return nil, err
	}
	return &SpreadsheetConfig{
		IngestionSpreadsheetID: ingest,
		PushSpreadsheetID:      push,
	}, nil
}

func (s *SheetStore) SetCode(code string) error {
	tok, err := s.AuthConfig.Exchange(context.TODO(), code)
	if err != nil {
		return err
	}
	jsonBytes, err := json.Marshal(tok)
	if err != nil {
		return err
	}
	return s.setTokenEntry(string(jsonBytes), "auth")
}

func (s *SheetStore) TeamsFromSheets() ([]*rcjpb.Team, error) {
	sheetsClient, err := s.getSheetsService()
	if err != nil {
		return nil, err
	}
	readRange := "Teams!A1:V"
	ingestionID, err := s.getTokenEntry("ingestion")
	if err != nil {
		return nil, err
	}
	resp, err := sheetsClient.Spreadsheets.Values.Get(ingestionID, readRange).Do()
	if err != nil {
		return nil, err
	}
	teams := []*rcjpb.Team{}
	for _, row := range resp.Values {
		team := &rcjpb.Team{
			Members: []*rcjpb.Member{},
		}
		for idx, col := range row {
			stringValue := col.(string)
			if idx == 0 {
				team.ImportId = stringValue
			} else if idx == 1 {
				team.Name = stringValue
			} else if idx == 2 {
				team.Institution = &rcjpb.Institution{
					Name: stringValue,
				}
			} else if idx == 5 {
				team.Division = stringValue
			} else if idx == 6 && stringValue != "" {
				member := &rcjpb.Member{
					Name:   stringValue,
					Gender: rcjpb.Member_UNSPECIFIED,
				}
				team.Members = append(team.Members, member)
			} else if idx == 9 && stringValue != "" {
				member := &rcjpb.Member{
					Name:   stringValue,
					Gender: rcjpb.Member_UNSPECIFIED,
				}
				team.Members = append(team.Members, member)
			} else if idx == 12 && stringValue != "" {
				member := &rcjpb.Member{
					Name:   stringValue,
					Gender: rcjpb.Member_UNSPECIFIED,
				}
				team.Members = append(team.Members, member)
			} else if idx == 15 && stringValue != "" {
				member := &rcjpb.Member{
					Name:   stringValue,
					Gender: rcjpb.Member_UNSPECIFIED,
				}
				team.Members = append(team.Members, member)
			} else if idx == 18 && stringValue != "" {
				member := &rcjpb.Member{
					Name:   stringValue,
					Gender: rcjpb.Member_UNSPECIFIED,
				}
				team.Members = append(team.Members, member)
			}
		}
		teams = append(teams, team)
	}
	return teams, nil
}

func (s *SheetStore) ExportToSheets(checkins []*rcjpb.Checkin, teams []*rcjpb.Team, divisions []*rcjpb.Division) error {
	type sheetLineEntry struct {
		TeamName    string
		Institution string
		MemberCount int
		CheckedIn   bool
		Comments    string
		DivisionId  string
	}
	teamMap := map[string]*sheetLineEntry{}
	for _, team := range teams {
		teamMap[team.GetId()] = &sheetLineEntry{
			TeamName:    team.GetName(),
			Institution: team.GetInstitution().GetName(),
			MemberCount: len(team.GetMembers()),
			DivisionId:  team.GetDivision(),
		}
	}
	for _, checkin := range checkins {
		if _, ok := teamMap[checkin.GetTeam().GetId()]; !ok {
			continue
		}
		entry := teamMap[checkin.GetTeam().GetId()]
		entry.CheckedIn = true
		entry.Comments = checkin.GetComments()
	}
	divMap := map[string][]*sheetLineEntry{}
	for _, entry := range teamMap {
		var divMatch *rcjpb.Division
		for _, div := range divisions {
			if div.GetId() == entry.DivisionId {
				divMatch = div
				break
			}
		}
		if divMatch == nil {
			continue
		}
		if _, ok := divMap[divMatch.GetName()]; !ok {
			divMap[divMatch.GetName()] = []*sheetLineEntry{}
		}
		divMap[divMatch.GetName()] = append(divMap[divMatch.GetName()], entry)
	}
	data := []*sheets.ValueRange{}
	for divName, teamEntries := range divMap {
		vr := &sheets.ValueRange{
			Range: fmt.Sprintf("%s!A1:E", divName),
			Values: [][]interface{}{{
				"Team Name",
				"Institution",
				"Member Count",
				"Checked In",
				"Comments",
			}},
		}
		for _, teamEntry := range teamEntries {
			vr.Values = append(vr.Values, []interface{}{
				teamEntry.TeamName,
				teamEntry.Institution,
				teamEntry.MemberCount,
				teamEntry.CheckedIn,
				teamEntry.Comments,
			})
		}
		data = append(data, vr)
	}
	rb := &sheets.BatchUpdateValuesRequest{
		Data:             data,
		ValueInputOption: "RAW",
	}
	sheetsClient, err := s.getSheetsService()
	if err != nil {
		return err
	}
	// Check to see if all the sheets exist
	pushID, err := s.getTokenEntry("push")
	if err != nil {
		return err
	}
	meta, err := sheetsClient.Spreadsheets.Get(pushID).Do()
	if err != nil {
		return err
	}
	missingTabs := []string{}
	for divName, _ := range divMap {
		found := false
		for _, sheet := range meta.Sheets {
			if sheet.Properties.Title == divName {
				found = true
				break
			}
		}
		if !found {
			missingTabs = append(missingTabs, divName)
		}
	}
	if len(missingTabs) > 0 {
		requests := []*sheets.Request{}
		for _, missingTab := range missingTabs {
			requests = append(requests, &sheets.Request{
				AddSheet: &sheets.AddSheetRequest{
					Properties: &sheets.SheetProperties{
						Title: missingTab,
					},
				},
			})
		}
		bu := &sheets.BatchUpdateSpreadsheetRequest{
			Requests: requests,
		}
		_, err = sheetsClient.Spreadsheets.BatchUpdate(pushID, bu).Do()
		if err != nil {
			return err
		}
	}
	_, err = sheetsClient.Spreadsheets.Values.BatchUpdate(pushID, rb).Do()
	return err
}
