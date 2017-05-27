;; TODO
;; - what about those language filters??
;; - or get all properties??

(use awful srfi-69)

(load "sparql.scm")
(load "rdf.scm")
(load "rest.scm")
(load "threads.scm")

(development-mode? #t)
(debug-file "./debug.log")
(*print-queries?* #t)

(*default-graph* ("http://mu.semte.ch/application"))

(*sparql-endpoint* "http://127.0.0.1:8890/sparql?")

(define-namespace mu "http://mu.semte.ch/application/")




