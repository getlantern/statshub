package main

import (
	"log"
)

func main() {
	myString := "hello world"
	doStuff(&ASpecificThingy{name: myString})
}

func doStuff(thingy MyThingy) {
	log.Printf("Name is: %s", thingy.MyName())
}

type MyThingy interface {
	MyName() string
}

type ASpecificThingy struct {
	name string
}

func (self *ASpecificThingy) MyName() string {
	return self.name
}
