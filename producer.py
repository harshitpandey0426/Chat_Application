# importing the required module 
from kafka import KafkaConsumer
from json import loads
from time import sleep
from json import dumps
from kafka import KafkaProducer

def validation(varify):
    file1=open("login.txt","r")

    validate=False
    
    lines1=file1.readlines()
#    print(lines1)
    for line1 in lines1:
#        print(line1)
        
        if(line1.strip()==varify):
            validate=True
            
    if validate==False:
        print("Invalid User")
        exit()
    else:
        print("validated")


print("Enter Id")
topic=input()
validation(topic)

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

#for e in range(10):
#    data = {'number' : e}
#    print(data)
#    producer.send('numtest', value=data,)
#    sleep(2)
#    

consumer = KafkaConsumer(
     topic,
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='latest',
     enable_auto_commit=True,
     value_deserializer=lambda x: loads(x.decode('utf-8')))

#while(1):
#    data=input()
#    producer.send('numtest',value=data)
#    sleep(1)

def to_recv():
    for message in consumer:
        message = message.value
        print('{} '.format(message))
        if(message=="over"):
            break
    
def to_send():
    print("to whom?")
    topic=input()
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x: 
                             dumps(x).encode('utf-8'))
    while(1):
        data=input()
        producer.send(topic,value=data)
        if(data=="over"):
            break
        sleep(1)
 
while(1):
    print("What to do?")
    inp=input()    
    if(inp=="send"):
        to_send()
    if(inp=="recv"):
        to_recv()