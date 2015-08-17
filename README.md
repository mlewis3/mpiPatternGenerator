# mpiPatternGenerator
This repository uses MPI to create a pattern file from a RDF graph specified in the 
N-Triple format. N-Triples is a line-based, plain text format for encoding an RDF Graph (wwww.w3.org/TR/n-triples)
The patterns generated <subject-type predicate-string object-type > here type refers to all the URIs 
associated with the pattern: <uri , predicate-string, literal> . For example the triple :Jane employed-at  "UIC"
and :Mark employed at "Loyola" signifys the URI :Jane belongs to the pattern group employed-at UIC and Mark 'employed-at Loyola' 
The triple  :Jane works-with :Mark can be under the triple pattern "employed-at UIC" works-with "Employed at Loyola".

