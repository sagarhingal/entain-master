package db

import (
	"math/rand"
	"time"

	"syreclabs.com/go/faker"
)

// Sport represents a type of sport
type Sport string

// Sample sports types
const (
	Football   Sport = "Football"
	Basketball Sport = "Basketball"
	Tennis     Sport = "Tennis"
	Soccer     Sport = "Soccer"
	Cricket    Sport = "Cricket"
	DOTA       Sport = "DOTA"
	CS         Sport = "Counter Strike 2"
	Boxing     Sport = "Boxing"
)

func (r *sportsRepo) seed() error {
	statement, err := r.db.Prepare(`CREATE TABLE IF NOT EXISTS sports (id INTEGER PRIMARY KEY, event_id INTEGER, sports_type TEXT, name TEXT, number INTEGER, advertised_start_time DATETIME)`)
	if err == nil {
		_, err = statement.Exec()
	}

	for i := 1; i <= 100; i++ {
		statement, err = r.db.Prepare(`INSERT OR IGNORE INTO sports(id, event_id, sports_type, name, number, advertised_start_time) VALUES (?,?,?,?,?,?)`)
		if err == nil {
			_, err = statement.Exec(
				i,
				faker.Number().Between(1, 10),
				generateSportType(),
				faker.Team().Name(),
				faker.Number().Between(1, 10),
				faker.Time().Between(time.Now().AddDate(0, 0, -1), time.Now().AddDate(0, 0, 2)).Format(time.RFC3339),
			)
		}
	}

	return err
}

// generateSportType chooses a random type of sport based on the given list of options
func generateSportType() Sport {
	source := rand.NewSource(time.Now().UnixNano())
	rng := rand.New(source)
	sports := []Sport{Football, Basketball, Tennis, Soccer, Cricket, DOTA, CS, Boxing}
	return sports[rng.Intn(len(sports))]
}
