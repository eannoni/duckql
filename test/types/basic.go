package types

type User struct {
	ID           int
	Name         string
	Email        string
	PasswordHash string `ddl:"-"`
}

type Temperature struct {
	Measurement float64
}

type Account struct {
	ID             int
	Username       string
	Email          string
	OrganizationID int
}

type Organization struct {
	ID   int
	Name string
}
