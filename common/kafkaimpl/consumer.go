package kafkaimpl

type Consumer struct {
}

func CreateNewConsumer(topics []string /* config *kafkaConfig*/) *Consumer {
	consumer := &Consumer{}
	for _, topic := range topics {
		consumer.AddTopic(topic)
	}
	return consumer
}

func (consumer *Consumer) AddTopic(topic string) {

}

func (consumer *Consumer) Start() {

}

func (consumer *Consumer) Stop() {

}
