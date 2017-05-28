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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Model

(define-record resource name type base base-prefix graph graph-type properties)

(define-record item resource id properties)

(define *realms* (make-parameter '()))

(define (define-realm name value)
  (*realms* (cons (cons name value) (*realms*))))

(define (get-realm-by-name name)
  (alist-ref name (*realms*)))

(define *resources* (make-parameter '()))

(define (define-resource name properties)
  (*resources* (cons name (*resources*)))
  (put! name 'resource
	(make-resource name
		       (alist-ref 'predicate properties)
		       (alist-ref 'base properties)
		       (alist-ref 'base-prefix properties)
		       (alist-ref 'graph properties)
		       (alist-ref 'graph-type properties)
		       (alist-ref 'properties properties))))

(define (get-resource-by-name name) (get name 'resource))

(define (get-resource-by-uri uri #!optional (resources (*resources*)))
  (if (null? resources)
      #f
      (let ((resource (get-resource-by-name (car resources))))
	(if (equal? uri (write-uri (reify (resource-type resource))))
	    resource
	    (get-resource-by-uri uri (cdr resources))))))

(define (resource-property resource property)
  (assoc property (resource-properties resource)))

(define (property-name property)
  (car property))

(define (property-resource property)
  (car-when (alist-ref 'resource (cdr property))))

(define (property-predicate property)
  (car-when (alist-ref 'predicate (cdr property))))

(define (property-inverse? property)
  (car-when (alist-ref 'inverse?  (cdr property))))

(define (property-multiple? property)
  (car-when (alist-ref 'multiple? (cdr property))))

(define (resource-property-predicate resource property)
  (property-predicate
   (assoc property (resource-properties resource))))

(define (resource-property-resource resource property)
  (property-resource
   (assoc property (resource-properties resource))))

(define (resource-property-multiple? resource property)
  (property-multiple?
   (assoc property (resource-properties resource))))

(define (resource-property-inverse? resource property)
  (property-inverse?
   (assoc property (resource-properties resource))))

(define (get-graph-query resource realm)
  (let ((graph-type (reify (resource-graph-type resource))))
    (select-triples "?graph"
		    (format #f (conc "?graph gd:type ~A .~%"
				     "?graph gd:realm ~A")
			    graph-type (reify realm)))))

(define (get-property-by-predicate property-list predicate)
  (cond ((null? property-list) #f)
	((equal? (reify (property-predicate (car property-list))) predicate)
	 (caar property-list))
	(else (get-property-by-predicate (cdr property-list) predicate))))
	 

(define (get-resource-graph resource realm)
  (or (resource-graph resource)
      (car (query-with-vars (graph) (get-graph-query resource realm) graph))))

(define (get-resource-graph-by-name name realm)
  (get-resource-graph (get name 'resource) realm))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Queries

(define (get-properties-query1 realm resource id)
  (let ((properties (resource-properties resource))
	(graph (get-resource-graph resource realm)))
    (select-triples
     "?p, ?o"
     (string-join
      (map (lambda (property)
	     (format #f "{ ~A ~A ?o . ~A ?p ?o }~%"
		     (reify id)
		     (reify (property-predicate property))
		     (reify id)))
		 properties)
	    " UNION ")
     #:graph (reify graph))))

(define (get-property-graph realm property)
  (get-resource-graph
   (get-resource-by-name
    (property-resource property)) realm))

(define (resource-property-graphs realm resource)
  (let ((inverse-properties (filter property-inverse? (resource-properties resource))))
    (map (lambda (property)
	   (get-property-graph realm property))
	 inverse-properties)))
     
(define (get-properties-query realm resource id)
  (select-from
   "?p, ?o"
   (property-clauses realm resource id)
   #:graph (reify (get-resource-graph resource realm))
   #:named-graphs (resource-property-graphs realm resource)))

(define (property-clauses realm resource id)
  (let ((graph (get-resource-graph resource realm))
	(properties (resource-properties resource)))
    (string-join
     (map (lambda (property)
	    (if (property-inverse? property)
		(let ((property-graph
		       (get-resource-graph
			(get-resource-by-name (property-resource property)) realm)))
		  (format #f "{ ~A { ?o ~A ~A . ?o ?p ~A } }~%"
			  (if (equal? property-graph graph) ""
			      (format #f " GRAPH ~A " (reify property-graph)))
			  (reify (property-predicate property))
			  (reify id)
			  (reify id)))
		(format #f "{ ~A ~A ?o . ~A ?p ?o }~%"
		    (reify id)
		    (reify (property-predicate property))
		    (reify id))))
	  properties)
     " UNION ")))

(define (delete-inverse-property-query realm resource id property)
  (let ((statement (format #f "?s ~A ~A "
			   (reify (property-predicate property)) (reify id))))
    (delete-from
     statement
     #:where statement
     #:graph  (get-resource-graph
	       (get-resource-by-name (property-resource property)) realm))))

(define (delete-properties-query-statement realm resource id property-values #!optional full?)
  (conc "?s ?p ?o .\n"
	(if full? (format #f " { ~A a ?o .} UNION " (reify id)) "")
	(string-join
	 (map (lambda (property-value)
		(format #f "{ ~A ~A ~A } ~%"
			(reify id)
			(reify (resource-property-predicate
				resource (car property-value)))
			(if (null? (cdr property-value))
			    "?o"
			    (reify (cdr property-value)))))
	      property-values)
	 " UNION ")))

(define (delete-properties-query realm resource id property-values #!key full?)
  (let ((graph (get-resource-graph resource realm)))
    (delete-triples
     "?s ?p ?o"
     #:where (delete-properties-query-statement realm resource id property-values full?)
     #:graph (reify graph))))

(define (insert-properties-query realm resource id property-values)
  (print "PROPS O"  property-values)
  (let ((graph (get-resource-graph resource realm)))
    (insert-triples
     (string-join
      (map (lambda (property-value)
	     (format #f "~A ~A ~A .~%"
		     (reify id)
		     (reify (resource-property-predicate
			     resource (car property-value)))
		     (reify (cdr property-value))))
	   property-values))
     #:graph (reify graph))))

(define (get-items-query realm resource)
  (let ((type (resource-type resource))
	(graph (get-resource-graph resource realm)))
    (select-triples
     "?s"
     (format #f "?s a ~A~%" (reify type))
     #:graph graph)))

(define (get-links-query realm resource id link-type linked-resource #!optional inverse?)
  (let ((graph (if inverse?
		   (get-resource-graph linked-resource realm)
		   (get-resource-graph resource realm))))
    (select-triples
     "?o"
     (if inverse?
	 (format #f "?o ~A ~A~%" (reify link-type) (reify id))
	 (format #f "~A ~A ?o~%" (reify id) (reify link-type)))
     #:graph graph)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Call Implementation

(define (get-item realm resource id)
  (make-item resource id (get-properties realm resource id)))

(define (delete-item realm resource id)
  (let-values (((inverse-properties properties)
		(partition property-inverse? (resource-properties resource))))
    (map (lambda (property)
	   (sparql/update
	   (delete-inverse-property-query realm resource id property)))
	 inverse-properties)
    (sparql/update
     (delete-properties-query realm resource id
			      (map list (map car properties))
			      #:full? #t))))

(define (get-properties realm resource id)
  (let ((properties (resource-properties resource)))
    (fold-alist
     (query-with-vars
      (property value)
      (get-properties-query realm resource id)
      (cons (get-property-by-predicate properties property)
	    (rdf->json value))))))

(define (update-properties realm resource id property-values)
  (let ((unique-property-names
	 (filter
	  (lambda (property-value)
	    (not
	     (property-multiple?
	      (resource-property resource property-value))))
	  (map car property-values))))
    (sparql/update
     (delete-properties-query realm resource id (map list unique-property-names)))
    (sparql/update
     (insert-properties-query realm resource id property-values))))

(define (get-links realm resource id link-type linked-resource #!optional inverse?)
  (let ((properties (resource-properties resource)))
    (query-with-vars
     (element)
     (get-links-query realm resource id link-type linked-resource inverse?)
     element)))

(define (get-items realm resource)
  (let ((properties (resource-properties resource)))
    (query-with-vars
     (element)
     (get-items-query realm resource)
     element)))

;; (get-resource-by-name resource-name)))
(define (get-linked-items realm resource id link-type linked-resource #!optional inverse?)
  (map (lambda (element)
	 (get-item realm linked-resource element))
       (get-links realm resource id link-type linked-resource inverse?)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; API Calls

(define (list-call realm-name resource-name)
  (get-items (get-realm-by-name realm-name)
	     (get-resource-by-name resource-name)))

(define (show-call realm-name resource-name id-stub)
  (let* ((realm (get-realm-by-name realm-name))
	(resource (get-resource-by-name resource-name))
	(id (list (resource-base-prefix resource) id-stub)))
    (get-item realm resource id)))

(define (links-call realm-name resource-name id-stub link)
  (let* ((realm (get-realm-by-name realm-name))
	 (resource (get-resource-by-name resource-name))
	 (id (list (resource-base-prefix resource) id-stub))
	 (link-property (resource-property resource link))
	 (link-type (property-predicate link-property))
	 (linked-resource (get-resource-by-name (property-resource link-property)))
	 (link-inverse? (property-inverse? link-property)))
    (get-linked-items realm resource id link-type linked-resource link-inverse?)))

;; (define (create-call realm-name resource-name item-object)

(define (extract-properties resource item-object)
  (filter values
	  (map (lambda (prop-val)
		 (and (resource-property resource (car prop-val)) prop-val))
	       item-object)))

(define (update-call realm-name resource-name id-stub item-object)
  (let* ((realm (get-realm-by-name realm-name))
	 (resource (get-resource-by-name resource-name))
	 (id (list (resource-base-prefix resource) id-stub))
	 (property-values (extract-properties resource item-object)))
    (print realm "/" resource "/" id)
    (update-properties realm resource id property-values)))

(define (delete-call realm-name resource-name id-stub)
  (let* ((realm (get-realm-by-name realm-name))
	(resource (get-resource-by-name resource-name))
	(id (list (resource-base-prefix resource) id-stub)))
    (delete-item realm resource id)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Data Format

(define (item->json-ld1 resource id properties) ; resource id realm)
  (let ((props (resource-properties resource)))
    `((@id . ,(write-uri (reify id)))
      (@type . ,(write-uri (reify (resource-type resource))))
      ,@properties ;(get-properties resource id realm)
      (@context ,@(map (lambda (property)
			(cons (car property)
			      (lookup-namespace (cadr property))))
		       props)))))

;; put things in contexts?
(define (item->json-ld item)
  (let* ((resource (item-resource item))
	   (props (resource-properties resource)))
    `((@id . ,(write-uri (item-id item)))
      (@type . ,(write-uri (reify (resource-type resource))))
      ,@(item-properties item)
      (@context ,@(map (lambda (property)
			(cons (car property)
			      (lookup-namespace (cadr property))))
		       props)))))

;; to do: handle @contexts
(define (json-ld->item realm json-ld)
  (let ((resource (get-resource-by-uri (alist-ref '@type json-ld)))
	(id (read-uri (alist-ref '@id json-ld))))
    (make-item resource id (get-properties realm resource id))))

;; (define put-properties)


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Tests

(define-realm 'AH 'eurostat:AlbertHeijn)

;; change properties format to:
;; (gtin (type (mu "amount")) (inverse? #f) (unique? #t))
;; then collate for non-unique properties
(define-resource 'product `((type . (eurostat Product))
			    (graph-type . (eurostat ProductsGraph))
			    (base . "http://mu.semte.ch/eurostat")
			    (base-prefix . eurostat)
			    (properties (gtin (predicate(mu "gtin")))
					(amount (predicate(mu "amount")))
					(description (predicate(mu "description"))
						     (multiple? #t))
					(ecoicop (predicate(mu "class"))
						 (resource class)))))

(define-resource 'class `((type. (eurostat ECOICOP))
			  (graph . (eurostat ECOICOP))
			  (base . "http://mu.semte.ch/eurostat")
			  (base-prefix . eurostat)
			  (properties (name (predicate (mu "name")))
				      (product (predicate (mu "class"))
					       (resource product)
					       (inverse? #t)))))
;; Tests
;;
;; (get-linked-items '(eurostat AlbertHeijn) (get-resource-by-name 'product) (eurostat 'product1) 'class)
;; (get-properties  'eurostat:AlbertHeijn (get-resource-by-name 'product) (eurostat 'product1))
;; (item->json-ld
;;  (get-resource-by-name 'product) '(eurostat product1)
;;  (get-properties 'eurostat:AlbertHeijn (get-resource-by-name 'product) (eurostat 'product1) ))
;;
;; (define o (get-item 'eurostat:AlbertHeijn (get-resource 'product) (eurostat 'product1)))
; (json->string (item->json-ld o))
;; (equal? o (json-ld->item '(eurostat AlbertHeijn) (item->json-ld o)))
