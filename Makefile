all: Server.class UserNode.class

%.class: %.java
	javac $<

clean:
	rm -f *.class

# # source files in the current directory.
# SOURCES := $(wildcard *.java) # $(wildcard common/*.java)

# # convert .java files to .class files.
# CLASSES := $(SOURCES:.java=.class)

# all:
# 	javac -d . $(SOURCES)

# # remove all compiled class files.
# clean:
# 	rm -f *.class