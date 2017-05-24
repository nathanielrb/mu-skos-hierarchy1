;; TODO
;; - what about those language filters??
;; - or get all properties??

(use awful)

(load "sparql.scm")
(load "rdf.scm")
(load "rest.scm")

(*default-graph* "http://data.europa.eu/eurostat/ECOICOP")

(*sparql-endpoint* "http://172.31.63.185:8890/sparql?")

(define-namespace taxonomy "http://data.europa.eu/eurostat/id/taxonomy/")

(define-namespace ecoicop "http://data.europa.eu/eurostat/id/taxonomy/ECOICOP/concept/")

(define-namespace skos "http://www.w3.org/2004/02/skos/core#")

(development-mode? #t)

(debug-file "./debug.log")

;; (get-environment-variable "taxonomy")
(define taxonomy (make-parameter (taxonomy "ECOICOP")))

(define (descendants-query ecoicop)
  (select-triples
   "?x"
   (format #f (conc "?x skos:inScheme <~A>.~%"
                     "?x skos:broader <~A>.~%")
           (taxonomy) ecoicop)))

(define (ancestors-query ecoicop)
  (select-triples
   "?x"
   (format #f (conc "?x skos:inScheme <~A>.~%"
                     "<~A> skos:broader ?x .~%")
           (taxonomy) ecoicop)))

(define (properties-query ecoicop)
  (select-triples
   "?name, ?description"
   (format #f (conc "<~A> skos:altLabel ?name.~%"
                     "<~A> skos:prefLabel ?description.~%"
                     "FILTER (lang(?name) = 'en')~%"
                     "FILTER (lang(?description) = 'en')~%")
           ecoicop ecoicop)))

(define (get-descendants ecoicop)
  (query-with-vars (x) (descendants-query ecoicop) x))

(define (get-ancestors ecoicop)
  (query-with-vars (x) (ancestors-query ecoicop) x))

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
      (json->string (ecoicop-forward-tree (ecoicop ($path 'id)) 
                                          (and levels (string->number levels))))))
  no-template: #t)

(define-rest-page (($path "/hierarchies/:id/ancestors"))
  (lambda ()
    (let ((levels ($ 'levels)))
      (json->string (ecoicop-reverse-tree (ecoicop ($path 'id))
                                          (and levels (string->number levels))))))
  no-template: #t)


