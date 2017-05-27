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

(define-record object resource id properties)

(define *resources* (make-parameter '()))

(define (define-resource name properties)
  (*resources* (cons name (*resources*)))
  (put! name 'resource
	(make-resource name
		       (assoc-val 'class properties)
		       (assoc-val 'base properties)
		       (assoc-val 'base-prefix properties)
		       (assoc-val 'graph properties)
		       (assoc-val 'graph-type properties)
		       (assoc-val 'properties properties))))

(define (get-resource name) (get name 'resource))

(define (get-resource-by-uri uri #!optional (resources (*resources*)))
  (if (null? resources)
      #f
      (let ((resource (get-resource (car resources))))
	(if (equal? uri (write-uri (reify (resource-class resource))))
	    resource
	    (get-resource-name-by-uri uri (cdr resources))))))

(define (resource-property resource property)
  (assoc-val property (resource-properties resource)))

(define (property-class property)
  (car (assoc-val 'class property)))

(define (property-inverse? property)
  (car (assoc-val 'inverse? property)))

(define (property-unique? property)
  (car (assoc-val 'unique? property)))

(define (resource-property-class resource property)
  (property-class
   (assoc-val property
	      (resource-properties resource))))

(define (resource-property-unique? resource property)
  (property-unique?
	     (assoc-val property (resource-properties resource))))

(define (resource-property-inverse? resource property)
  (property-inverse?
   (assoc-val property (resource-properties resource))))

(define (get-graph-query resource realm)
  (let ((graph-type (reify (resource-graph-type resource))))
    (select-triples "?graph"
		    (format #f (conc "?graph gd:type ~A .~%"
				     "?graph gd:realm ~A")
			    graph-type (reify realm)))))

(define (get-property-by-predicate property-list predicate)
  (cond ((null? property-list) #f)
	((equal? (reify (property-class (cdar property-list))) predicate)
	 (caar property-list))
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
	     (print "PROP " property)
	     (format #f "{ ~A ~A ?o . ~A ?p ?o }~%"
		     (reify id)
		     (reify (property-class (cdr property)))
		     (reify id)))
		 properties)
	    " UNION ")
     #:graph (reify graph))))

(define (delete-properties-query realm resource id properties)
  (let ((graph (get-resource-graph resource realm)))
    (delete-triples
     "?s ?p ?o"
     #:where (conc "?s ?p ?o .\n"
		   (string-join
		    (map (lambda (property)
			   (format #f "{ ~A ~A ~A } ~%"
				   (reify id)
				   (reify (resource-property-class resource property))
				   (if (null? (cdr property))
				       "?o"
				       (reify (cdr property)))))
			 properties)
		    " UNION "))
     #:graph (reify graph))))

(define (insert-properties-query realm resource id properties)
  (let ((graph (get-resource-graph resource realm)))
    (insert-triples
     (string-join
      (map (lambda (property)
	     (format #f "~A ~A ~A .~%"
		     (reify id)
		     (reify (resource-property-class resource (car property)))
		     (reify (cdr property))))
	   properties))
     #:graph (reify graph))))

(define (get-links-query realm resource id link)
  (let ((properties (resource-properties resource))
	(graph (get-resource-graph resource realm))
	(pred (reify link)))
    (select-triples
     "?o"
     (format #f "~A ~A ?o~%" id pred)
     #:graph graph)))

(define (get-object realm resource-name id)
  (let ((resource (get-resource resource-name)))
    (make-object resource id (get-properties realm resource id))))
   
(define (get-properties realm resource id)
  (let ((properties (resource-properties resource)))
    (query-with-vars
     (property value)
     (get-properties-query realm resource id)
     (cons (get-property-by-predicate properties property)
	   (rdf->json value)))))

(define (update-properties realm resource id properties)
  (sparql/update
    (insert-properties-query realm resource id properties)))

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

(define (object->json-ld1 resource id properties) ; resource id realm)
  (let ((props (resource-properties resource)))
    `((@id . ,(write-uri (reify id)))
      (@type . ,(write-uri (reify (resource-class resource))))
      ,@properties ;(get-properties resource id realm)
      (@context ,@(map (lambda (property)
			(cons (car property)
			      (lookup-namespace (cadr property))))
		       props)))))

;; put things in contexts?
(define (object->json-ld object)
  (let* ((resource (object-resource object))
	   (props (resource-properties resource)))
    `((@id . ,(write-uri (object-id object)))
      (@type . ,(write-uri (reify (resource-class resource))))
      ,@(object-properties object)
      (@context ,@(map (lambda (property)
			(cons (car property)
			      (lookup-namespace (cadr property))))
		       props)))))

;; to do: handle @contexts
(define (json-ld->object realm json-ld)
  (let ((resource (get-resource-by-uri (assoc-val '@type json-ld)))
	(id (read-uri (assoc-val '@id json-ld))))
    (make-object resource id (get-properties realm resource id))))

;; (define put-properties)


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Tests

;; change properties format to:
;; (gtin (class (mu "amount")) (inverse? #f) (unique? #t))
;; then collate for non-unique properties
(define-resource 'product `((class . (eurostat Product))
			    (graph-type . (eurostat ProductsGraph))
			    (base . "http://mu.semte.ch/eurostat")
			    (base-prefix . "eurostat")
			    (properties (gtin (class (mu "gtin")))
					(amount (class (mu "amount")))
					(description (class (mu "description")))
					(ecoicop (class (mu "ecoicop"))))))


(define-resource 'class `((class . (eurostat ECOICOP))
			  (graph . (eurostat ECOICOP))
			  (base . "http://mu.semte.ch/eurostat")
			  (base-prefix . "eurostat")
			  (properties (name (class (mu "name"))))))
;; Tests
;;
;; (get-linked-objects '(eurostat AlbertHeijn) (get-resource 'product) (eurostat 'product1) 'class)
;; (get-properties  'eurostat:AlbertHeijn (get-resource 'product) (eurostat 'product1))
;; (object->json-ld
;;  (get-resource 'product) '(eurostat product1)
;;  (get-properties 'eurostat:AlbertHeijn (get-resource 'product) (eurostat 'product1) ))
;;
;; (define o (get-object 'eurostat:AlbertHeijn 'product (eurostat 'product1)))
; (json->string (object->json-ld o))
;; (equal? o (json-ld->object '(eurostat AlbertHeijn) (object->json-ld o)))
