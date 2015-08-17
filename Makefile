#
#  mpiPatternGenerator Makefile
#
#
#

MPICXX      = mpicxx
CPLUSFLAGS  = -O2 -g -qmaxmem=-1 -DDEBUG


COMPILE_CPLUS  = $(MPICXX)  $(CPLUSFLAGS) -c
LINK           = $(MPICXX) $(FCFLAGS)

SRCS = mantona.cpp \
       rdfReader.cpp \
       pathStructure.cpp

OBJS = $(SRCS:.cpp=.o)

TARGET = patternGenerator

all: $(TARGET)

%.o:%.cpp
	$(COMPILE_CPLUS) $<

$(TARGET): $(OBJS)
	$(LINK) $(OBJS) -o $(TARGET)

clean: 
	/bin/rm -f $(OBJS) $(TARGET)

