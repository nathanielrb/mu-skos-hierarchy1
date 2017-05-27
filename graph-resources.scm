;; TODO
;; - what about those language filters??
;; - or get all properties??

(use awful srfi-69)

(load "utilities.scm")
(load "sparql.scm")
(load "rest.scm")
;(load "threads.scm")

(development-mode? #t)
(debug-file "./debug.log")
(*print-queries?* #t)

(*default-graph* '<http://mu.semte.ch/graph-resources/>)

(*sparql-endpoint* "http://127.0.0.1:8890/sparql?")

(define-namespace mu "http://mu.semte.ch/application/")

(define-namespace gd "http://mu.semte.ch/graph-resources/")

(define-namespace eurostat "http://mu.semte.ch/eurostat/")


(define-record resource name class base base-prefix graph graph-type properties)

(define (define-resource name properties)
  (put! name 'resource
	(make-resource name
		       (assoc-val 'class properties)
		       (assoc-val 'base properties)
		       (assoc-val 'base-prefix properties)
		       (assoc-val 'graph properties)
		       (assoc-val 'graph-type properties)
		       (assoc-val 'properties properties))))

(define (get-resource name) (get name 'resource))

(define (get-graph-query resource realm)
  (let ((graph-type (reify (resource-graph-type resource))))
    (select-triples "?graph"
		    (format #f (conc "?graph gd:type ~A .~%"
				     "?graph gd:realm ~A")
			    graph-type (reify realm)))))

(define (get-property-by-predicate property-list predicate)
  (cond ((null? property-list) #f)
	((equal? (reify (cdar property-list)) predicate) (caar property-list))
	(else (get-property-by-predicate (cdr property-list) predicate))))
	 

(define (get-resource-graph resource realm)
  (or (resource-graph resource)
      (car (query-with-vars (graph) (get-graph-query resource realm) graph))))

(define (get-resource-graph-by-name name realm)
  (get-resource-graph (get name 'resource) realm))

(define (get-properties-query realm resource id)
  (let ((properties (resource-properties resource))
	(graph (get-resource-graph resource realm)))
    (select-triples
     "?p, ?o"
     (string-join
	    (map (lambda (property)
		   (let ((pred (reify (cdr property))))
		     (format #f "{ ~A ~A ?o . ~A ?p ?o }~%" (reify id) pred (reify id))))
		 properties)
	    " UNION ")
     #:graph (reify graph))))

(define (get-links-query realm resource id link)
  (let ((properties (resource-properties resource))
	(graph (get-resource-graph resource realm))
	(pred (reify link)))
    (select-triples
     "?o"
     (format #f "~A ~A ?o~%" id pred)
     #:graph graph)))

(define (get-properties realm resource id)
  (let ((properties (resource-properties resource)))
    (query-with-vars
     (property value)
     (get-properties-query realm resource id)
     (cons (get-property-by-predicate properties property)
	   (rdf->json value)))))

(define (get-links realm resource id link)
  (let ((properties (resource-properties resource)))
    (query-with-vars
     (element)
     (get-links-query realm resource id (assoc-val link properties))
     element)))

(define (get-linked-object-properties realm resource id link)
  (map (lambda (element)
	 (get-properties realm (get-resource link) element))
       (get-links realm resource id link)))

(define (object->json-ld resource id properties) ; resource id realm)
  (let ((props (resource-properties resource)))
    `((@id . ,(write-uri (reify id)))
      (@type . ,(write-uri (reify (resource-class resource))))
      ,@properties ;(get-properties resource id realm)
      (@context ,@(map (lambda (property)
			(cons (car property)
			      (lookup-namespace (cadr property))))
		      props)))))       

;; todo: refactor properties/linked-properties/object->json

;; (define (json-ld->object)

;; test:
;; (json-ld->object
;; (define put-properties)


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Tests

(define-resource 'product `((class . (eurostat Product))
			    (graph-type . (eurostat ProductsGraph))
			    (base . "http://mu.semte.ch/eurostat")
			    (base-prefix . "eurostat")
			    (properties (gtin . (mu "gtin"))
					(amount . (mu "amount"))
					(description . (mu "description"))
					(class . (mu "class")))))

(define-resource 'class `((class . (eurostat ECOICOP))
			    (graph . (eurostat ECOICOP))
			    (base . "http://mu.semte.ch/eurostat")
			    (base-prefix . "eurostat")
			    (properties (name . (mu "name")))))

;; (get-linked-objects '(eurostat AlbertHeijn) (get-resource 'product) (eurostat 'product1) 'class)
;; (get-properties  'eurostat:AlbertHeijn (get-resource 'product) (eurostat 'product1))
;; (object->json-ld (get-resource 'product) '(eurostat product1) (get-properties 'eurostat:AlbertHeijn (get-resource 'product) (eurostat 'product1) ))
