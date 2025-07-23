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
