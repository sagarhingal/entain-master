package db

const (
	sportsList = "list"
)

func getSportsQueries() map[string]string {
	return map[string]string{
		sportsList: `
			SELECT 
				id, 
				event_id, 
				sports_type, 
				name,
				number,  
				advertised_start_time 
			FROM sports
		`,
	}
}
