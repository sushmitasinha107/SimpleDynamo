# SimpleDynamo

The main goal is to provide both availability and linearizability at the same time. In other words, your implementation should always perform read and write operations successfully even under failures. At the same time, a read operation should always return the most recent value. This project is a part of course CSE 586 under Steve Ko.

Based on the amazon dynamo paper 
http://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf
