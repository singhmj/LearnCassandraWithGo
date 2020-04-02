package messages

type MessageType uint16

const (
	MessageTypeInvalid     MessageType = 0
	MessageTypeBlogPost    MessageType = 1
	MessageTypeBlogComment MessageType = 2
)
