name := "Paxos"

version := "1.0"

organization := "Dan"

scalaVersion := "2.9.0"

parallelExecution in Test := false

compileOrder := CompileOrder.JavaThenScala

libraryDependencies ++= Seq(
        "org.scalatest" % "scalatest_2.9.0" % "1.6.1"  
)
