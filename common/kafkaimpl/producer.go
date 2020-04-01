package kafkaimpl

type Producer struct {
}

func CreateNewProducer(topics []string /* config *kafkaConfig*/) *Consumer {
	producer := &Producer{}
	for _, topic := range topics {
		producer.AddTopic(topic)
	}
	return producer
}

func (producer *Producer) AddTopic(topic string) {

}

func (producer *Producer) Start() {

}

func (producer *Producer) Stop() {

}
