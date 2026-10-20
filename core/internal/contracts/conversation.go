package contracts

import "time"

type InputTemplate struct {
	Values   Data              `json:"values,omitempty"`
	Pointers map[string]string `json:"pointers,omitempty"`
}
type Subscription struct {
	ID        string        `json:"id"`
	Source    Ref           `json:"source"`
	Event     string        `json:"event"`
	Target    Ref           `json:"target"`
	Operation string        `json:"operation"`
	Input     InputTemplate `json:"input"`
	Removed   bool          `json:"removed,omitempty"`
}
type Receiver struct {
	Target    Ref           `json:"target"`
	Operation string        `json:"operation"`
	Input     InputTemplate `json:"input"`
}
type Participant struct {
	Ref      Ref       `json:"ref"`
	Receiver *Receiver `json:"receiver,omitempty"`
}
type Conversation struct {
	ID           string        `json:"id"`
	Title        string        `json:"title"`
	Participants []Participant `json:"participants"`
	Closed       bool          `json:"closed,omitempty"`
}
type Message struct {
	ID          string    `json:"message_id"`
	Author      Ref       `json:"author"`
	Recipients  []Ref     `json:"recipients"`
	Text        string    `json:"text"`
	Attachments []Ref     `json:"attachments,omitempty"`
	ReplyTo     string    `json:"reply_to,omitempty"`
	CallIDs     []string  `json:"call_ids"`
	At          time.Time `json:"at"`
	Result      *Result   `json:"result,omitempty"`
}
type SendMessage struct {
	ID          string `json:"id"`
	MessageID   string `json:"message_id"`
	Author      Ref    `json:"author"`
	Recipients  []Ref  `json:"recipients"`
	Text        string `json:"text"`
	Attachments []Ref  `json:"attachments,omitempty"`
	ReplyTo     string `json:"reply_to,omitempty"`
}
