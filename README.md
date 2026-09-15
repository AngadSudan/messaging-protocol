# Implementing the custom messaging Protocol

The program aims to register an origin and on the basis of that origin register consumers and producers. Even if they are on the different machines (socket programming) and once the message is received it will be saving it into a WAL file to make sure incase if my queue is down for some reason then the task aren't lost. Once the task is resolved it will added to a log file. 

The part is simple whenever a message comes it is stored in the form as it is and then decoded back to the original message before pushing it to the consumer.

## Configurable parameters
- MAX_RETRIES 
- MAX_TIME_MESSAGE_CONSUMED
- MAX_PRODUCER_SIZE
- MAX_QUEUE_SIZE
- MAX_CONSUMER_SIZE


