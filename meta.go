package dbx

type Meta struct {
	Header 		map[string]MetaField 	`json:"header"`
	Count		int 					`json:"count"`
}

type MetaField struct {
	Name 		string		`json:"name"`
	Type 		string		`json:"type"`
	Virtual 	bool 		`json:"virtual,omitempty"`
	Reference 	string		`json:"reference,omitempty"`
	Value 		interface{} `json:"value,omitempty"`
}

type Result struct {
	Meta 	Meta			`json:"meta"`
	Body 	interface{}		`json:"body"`
}
