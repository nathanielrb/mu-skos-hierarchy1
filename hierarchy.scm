;; TODO
;; - what about those language filters??
;; - or get all properties??

(use awful srfi-69)

(load "sparql.scm")
(load "rdf.scm")
(load "rest.scm")

(development-mode? #t)
(debug-file "./debug.log")
(*print-queries?* #f)

(*default-graph* (or (get-environment-variable "MU_DEFAULT_GRAPH")
                     "http://data.europa.eu/eurostat/ECOICOP"))

(*sparql-endpoint* (or (get-environment-variable "SPARQL_ENDPOINT")
                       "http://172.31.63.185:8890/sparql?"))

(define-namespace taxonomy "http://data.europa.eu/eurostat/id/taxonomy/")

(define-namespace skos "http://www.w3.org/2004/02/skos/core#")


(define node-namespace (or "http://data.europa.eu/eurostat/id/taxonomy/ECOICOP/concept/"))

(define-namespace ns node-namespace)

(define scheme (make-parameter
                (or (get-environment-variable "CONCEPT_SCHEME")
                    (taxonomy "ECOICOP"))))

(define (descendance-query vars scheme child parent)
  (select-triples
   vars
   (format #f (conc "~A skos:inScheme <~A>.~%"
                     "~A skos:broader ~A.~%")
           child scheme child parent)))

(define (descendants-query node)
  (descendance-query "?x" (scheme) "?x" node))

(define (ancestors-query node)
  (descendance-query "?x" (scheme) node "?x"))

;; this shouldn't be hard-coded
;; get all properties? or...
(define (properties-query node)
  (select-triples
   "?name, ?description"
   (format #f (conc "<~A> skos:altLabel ?name.~%"
                     "<~A> skos:prefLabel ?description.~%"
                     "FILTER (lang(?name) = 'en')~%"
                     "FILTER (lang(?description) = 'en')~%")
           node node)))

(define (get-descendants node)
  (let ((n (string->symbol node)))
    (or (get n 'descendants)
        (put! n 'descendants
              (query-with-vars (x) (descendants-query (conc "<" node ">")) x)))))

(define (get-ancestors node)
  (let ((n (string->symbol node)))
    (or (get n 'ancestors)
        (put! n 'ancestors
              (query-with-vars (x) (ancestors-query (conc "<" node ">")) x)))))

(define (car-when l)
  (if (null? l) '() (car l)))

(define (ecoicop-properties ecoicop)
  (append (list (cons 'id ecoicop))
          (car-when
           (query-with-vars
            (name description)
            (properties-query ecoicop)
            (list
             (cons 'name name)
             (cons 'description description))))))

(define (ecoicop-tree next-fn ecoicop #!optional levels)
  (if (eq? levels 0)
      (ecoicop-properties ecoicop)
      (append (ecoicop-properties ecoicop)
              (list (cons 'children
                          (list->vector
                           (map (lambda (e)
                                  (ecoicop-tree next-fn e (and levels (- levels 1))))
                                (next-fn ecoicop))))))))

(define (ecoicop-forward-tree ecoicop #!optional levels)
  (ecoicop-tree get-descendants ecoicop levels))

(define (ecoicop-reverse-tree ecoicop #!optional levels)
  (ecoicop-tree get-ancestors ecoicop levels))

(define-rest-page (($path "/hierarchies/:id/descendants"))
  (lambda ()
    (let ((levels ($ 'levels)))
      (json->string (ecoicop-forward-tree (ns ($path 'id)) 
                                          (and levels (string->number levels))))))
  no-template: #t)

(define-rest-page (($path "/hierarchies/:id/ancestors"))
  (lambda ()
    (let ((levels ($ 'levels)))
      (json->string (ecoicop-reverse-tree (ns ($path 'id))
                                          (and levels (string->number levels))))))
  no-template: #t)


