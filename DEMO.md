DEMO

central node = 24
regular node = 100
regular node = 350
regular node = 780

PART 1

> set TestKey 0 0 16
This demo rocks!

// maps to 780

> get TestKey

> delete TestKey

> get TestKey


PART 2
> set coolKey 0 0 4
test

// maps to 350

> kill node 350

> get coolKey

// should get the value from node 780


PART 3
> join node 350

> get demoTime

// should read it from node 350