#!/bin/bash
for i in {2..10}
do 
	cp ReplicaDriver1.java ReplicaDriver$i.java
	sed -i -e "s/ReplicaDriver1/ReplicaDriver$i/g" ReplicaDriver$i.java
	sed -i -e "s/Server1/Server$i/g" ReplicaDriver$i.java
done
