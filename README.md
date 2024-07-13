# LSMTree + SSTable Database Implementation

This is a crude implementation of a LSMTree and Sorted String Table 
based database. This is significantly faster at writing than reading, 
and this is by design. 

This is an implementation I've done in order to learn and
understand. 

Hopefully by the end of this implementation the performance will be 
kind of OK. 

At the time the feature list is:

Features
- SSTable
- WAL (but I got rid of it cause its slow)

Missing:
- No LSM Tree implementation 
- No Bloom Filter
- Leader election system
- Other DB concepts I have yet to learn