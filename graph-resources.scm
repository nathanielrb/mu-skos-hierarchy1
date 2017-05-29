;; TODO
;; - what about those language filters??
;; - or get all properties??

(use awful spiffy srfi-69)

(load "utilities.scm")
(load "sparql.scm")
(load "rest.scm")
;(load "threads.scm")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Model

(define *realms* (make-parameter '()))

(define (define-realm name value)
  (*realms* (cons (cons name value) (*realms*))))

(define (get-realm-by-name name)
  (alist-ref name (*realms*)))

(define *resources* (make-parameter '()))

(define-record item resource id properties)

(define-record resource name type base base-prefix graph graph-type properties)

(define (define-resource name properties)
  (*resources* (cons name (*resources*)))
  (put! name 'resource
	(make-resource name
		       (alist-ref 'type properties)
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

(define (get-property-resource property)
  (let ((res (property-resource property)))
    (and res (get-resource-by-name res))))

(define (property-predicate property)
  (car-when (alist-ref 'predicate (cdr property))))

(define (property-inverse? property)
  (car-when (alist-ref 'inverse?  (cdr property))))

(define (property-link? property)
  (property-resource property))

(define (property-multiple? property)
  (car-when (alist-ref 'multiple? (cdr property))))

(define (resource-property-predicate resource property)
  (property-predicate
   (assoc property (resource-properties resource))))

(define (resource-property-resource resource property)
  (property-resource
   (assoc property (resource-properties resource))))

(define (resource-property-link? resource property)
  (property-link?
   (assoc property (resource-properties resource))))

(define (resource-property-multiple? resource property)
  (property-multiple?
   (assoc property (resource-properties resource))))

(define (resource-property-inverse? resource property)
  (property-inverse?
   (assoc property (resource-properties resource))))

(define (get-graph-query resource realm)
  (let ((graph-type (resource-graph-type resource))) ;;  (reify (resource-graph-type resource))))
    (select-triples "?graph"
                    (s-triples
                     `((?graph gd:type ,graph-type)
                       (?graph gd:realm ,realm))))))

(define (get-property-by-predicate property-list predicate)
  (cond ((null? property-list) #f)
	((equal? (reify (property-predicate (car property-list))) predicate)
	 (caar property-list))
	(else (get-property-by-predicate (cdr property-list) predicate))))
	 

(define (get-resource-graph resource realm)
  (or (resource-graph resource)
      (car (query-with-vars (graph)
                            (get-graph-query resource realm) 
                            graph))))

(define (get-resource-graph-by-name name realm)
  (get-resource-graph (get name 'resource) realm))

(define (get-property-graph realm property)
  (get-resource-graph
   (get-resource-by-name
    (property-resource property)) realm))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Queries

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
  (let ((graph (reify (get-resource-graph resource realm)))
	(properties (resource-properties resource)))
    (union
     (map (lambda (property)
	    (if (property-inverse? property)
		(let ((property-graph
		       (get-resource-graph
			(get-resource-by-name (property-resource property)) realm)))
                  (graph-statement
                   (if (equal? property-graph graph) #f property-graph)
                   (s-triples
                    `((?o ,(property-predicate property) ,id)
                      (?o ?p ,id)))))
                (bracketed
                 (s-triples
                  `((,id ,(property-predicate property) ?o)
                    (,id ?p ?o))))))
	  properties))))

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

(define (insert-inverse-property-query realm resource id property-value)
  (let ((property (resource-property resource (car property-value))))
    (insert-triples
     (format #f "~A ~A ~A "
	     (reify (list (resource-base-prefix resource)
			  (cdr property-value)))
	     (reify (property-predicate property))
	     (reify id))
     #:graph  (get-resource-graph
	       (get-resource-by-name (property-resource property)) realm))))


(define (insert-properties-query realm resource id property-values)
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

(define (create-item-query realm resource id)
  (let ((graph (get-resource-graph resource realm)))
    (insert-triples
     (format #f "~A a ~A"
	     (reify id)
	     (reify (resource-type resource)))
     #:graph graph)))
	    
(define (get-items-query realm resource #!optional filters)
  (let ((type (resource-type resource))
	(graph (get-resource-graph resource realm)))
    (let-values (((named-graphs filter-statements)
                  (if filters
                      (filter-statements realm resource (car filters) (cdr filters) '?s)
                      (values '() #f))))
      (select-from
       "?s"
       (conc (format #f "?s a ~A~%" (reify type))
             filter-statements)
       #:named-graphs (map reify named-graphs)
       #:graph (reify graph)))))

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
;; Filters

(define (new-sparql-variable)
  (string->symbol (conc "?" (->string (gensym)))))

(define (filter-statements realm resource fields val last-var)
  (let loop ((fields fields)
             (lresource resource)
             (statements '())
             (var last-var)
             (named-graphs '()))
    (let* ((field (car fields))
           (property (resource-property lresource field))
           (new-var (new-sparql-variable))
           (lgraph (if (equal? lresource resource)
                       #f
                       (get-resource-graph lresource realm)))
           (new-triple (graph-statement
                        ;;(if (not (equal? lresource resource))
                        lgraph (triple var (property-predicate property) new-var))))
      (if (property-link? property)
          (loop (cdr fields) (get-resource-by-name (property-resource property))
                (cons new-triple statements)
                new-var (cons-when lgraph named-graphs))
          (values
           (delete-duplicates (cons-when lgraph named-graphs))
           (triples
            (cons
             (format #f "FILTER (regex(str(~A), \"~A\") ) ."
                     new-var val)
             (cons new-triple statements))))))))
                  

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Call Implementation

(define (get-item realm resource id)
  (make-item resource id (get-properties realm resource id)))

(define (delete-inverse-property realm resource id property)
  (sparql/update
   (delete-inverse-property-query realm resource id property)))

(define (delete-properties realm resource id properties #!optional full?)
    (sparql/update
     (delete-properties-query realm resource id properties #:full? full?)))

(define (delete-item realm resource id)
  (let-values (((inverse-properties properties)
		(partition property-inverse? (resource-properties resource))))
    (map (lambda (property)
	   (delete-inverse-property realm resource id property))
	 inverse-properties)
    (delete-properties realm resource id (map list (map car properties)) #:full? #t)))

(define (get-properties realm resource id)
  (let ((properties (resource-properties resource)))
    (fold-alist
     (query-with-vars
      (property value)
      (get-properties-query realm resource id)
      (cons (get-property-by-predicate properties property)
	    (rdf->json value))))))

(define (insert-inverse-property realm resource id property-value)
  (sparql/update
   (insert-inverse-property-query realm resource id property-value)))

(define (unique-pvs pvs properties)
  (filter
   (lambda (pv)
     (not
      (property-multiple?
       (assoc (car pv) properties))))
   (map list (map car pvs))))

(define (update-properties realm resource id property-values)
  (let ((properties (resource-properties resource)))
    (let-values (((inverse-property-values property-values)
		  (partition (lambda (pv)
			       (property-inverse?
				(assoc (car pv) properties)))
			     property-values)))
      (let ((unique-properties (unique-pvs property-values properties))
	    (unique-inverse-properties (unique-pvs inverse-property-values properties)))
	(delete-properties realm resource id  unique-properties)
	(map (lambda (property)
	       (print property)
	       (delete-inverse-property realm resource id
					(resource-property resource (car property)))) ;; ** !!
	     unique-inverse-properties)
	(map (lambda (pv)
	       (insert-inverse-property realm resource id pv))
	     inverse-property-values)
	(sparql/update 
	 (insert-properties-query realm resource id property-values))))))

(define (create-item realm resource id property-values)
  (let ((properties (resource-properties resource)))
    (let-values (((inverse-property-values property-values)
		  (partition (lambda (pv)
			       (property-inverse?
				(assoc (car pv) properties)))
			     property-values)))
      (sparql/update
       (create-item-query realm resource id))
      (map (lambda (pv)
	     (insert-inverse-property realm resource id pv))
	   inverse-property-values)
      (sparql/update 
       (insert-properties-query realm resource id property-values)))))

(define (update-properties1 realm resource id property-values)
  (let ((unique-property-names
	 (filter
	  (lambda (property-value)
	    (not
	     (property-multiple?
	      (resource-property resource property-value))))
	  (map car property-values))))
     (delete-properties realm resource id  (map list unique-property-names))
     ;;     (delete-properties-query realm resource id
    (sparql/update
     (insert-properties-query realm resource id property-values))))

(define (get-links realm resource id link-type linked-resource #!optional inverse?)
  (let ((properties (resource-properties resource)))
    (query-with-vars
     (element)
     (get-links-query realm resource id link-type linked-resource inverse?)
     element)))

(define (get-items realm resource #!optional filters)
  (let ((properties (resource-properties resource)))
    (map (lambda (item) (get-item realm resource item))
	 (query-with-vars
	  (element)
	  (get-items-query realm resource filters)
	  element))))


;; (get-resource-by-name resource-name)))
(define (get-linked-items realm resource id link-type linked-resource #!optional inverse?)
  (map (lambda (element)
	 (get-item realm linked-resource element))
       (get-links realm resource id link-type linked-resource inverse?)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; API Calls

(define (list-call realm-name resource-name #!optional filters)
  (get-items (get-realm-by-name realm-name)
	     (get-resource-by-name resource-name)
             filters))

(define-rest-page ((realm resource) "/resources/:realm/:resource")
  (lambda ()
    (list->vector
     (map item->json-ld
	  (list-call (string->symbol realm) (string->symbol resource))))))

(define (show-call realm-name resource-name id-stub)
  (let* ((realm (get-realm-by-name realm-name))
	(resource (get-resource-by-name resource-name))
	(id (list (resource-base-prefix resource) id-stub)))
    (get-item realm resource id)))

(define-rest-page ((realm resource id) "/resources/:realm/:resource/:id")
  (lambda ()
    (item->json-ld
     (show-call (string->symbol realm) (string->symbol resource) (string->symbol id)))))

(define (links-call realm-name resource-name id-stub link)
  (let* ((realm (get-realm-by-name realm-name))
	 (resource (get-resource-by-name resource-name))
	 (id (list (resource-base-prefix resource) id-stub))
	 (link-property (resource-property resource link))
	 (link-type (property-predicate link-property))
	 (linked-resource (get-resource-by-name (property-resource link-property)))
	 (link-inverse? (property-inverse? link-property)))
    (get-linked-items realm resource id link-type linked-resource link-inverse?)))

(define-rest-page ((realm resource id link) "/resources/:realm/:resource/:id/links/:link")
  (lambda ()
    (list->vector
     (map item->json-ld
	  (links-call (string->symbol realm) (string->symbol resource)
		      (string->symbol id) (string->symbol link))))))

(define (create-call realm-name resource-name id-stub item-object)
  (let* ((realm (get-realm-by-name realm-name))
	 (resource (get-resource-by-name resource-name))
	 (id (list (resource-base-prefix resource) id-stub))
	 (property-values (extract-properties resource item-object)))
    (create-item realm resource id property-values)
    '((success . "OK"))))

(define (read-request-json)
  (let* ((headers (request-headers (current-request)))
	 (content-length (header-value 'content-length headers))
	 (body (read-string content-length (request-port (current-request)))))
    (read-json body)))

(define-rest-page ((realm resource id) "/resources/:realm/:resource/:id")
  (lambda ()
    (create-call (string->symbol realm) (string->symbol resource) (string->symbol id)
		 (read-request-json)))
;;  (data  (with-input-from-string ($ 'data) read-json))
  method: 'POST)

;; todo: if null/#f, delete link
(define (update-call realm-name resource-name id-stub item-object)
  (let* ((realm (get-realm-by-name realm-name))
	 (resource (get-resource-by-name resource-name))
	 (id (list (resource-base-prefix resource) id-stub))
	 (property-values (extract-properties resource item-object)))
    (update-properties realm resource id property-values)
    '((success . "OK"))))

(define-rest-page ((realm resource id) "/resources/:realm/:resource/:id")
  (lambda ()
    (update-call (string->symbol realm) (string->symbol resource) (string->symbol id)
		 (read-request-json)))
  method: 'PATCH)

(define (delete-call realm-name resource-name id-stub)
  (let* ((realm (get-realm-by-name realm-name))
	(resource (get-resource-by-name resource-name))
	(id (list (resource-base-prefix resource) id-stub)))
    (delete-item realm resource id)))

(define-rest-page ((realm resource id) "/resources/:realm/:resource/:id")
  (lambda ()
    (delete-call (string->symbol realm) (string->symbol resource) (string->symbol id))
    '((success . "OK")))
  method: 'DELETE)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Data Format

(define (extract-properties resource item-object)
  (filter values
	  (map (lambda (pv)
		 (let ((property (resource-property resource (car pv))))
		   (if property
		       (let ((p-resource (get-property-resource property)))
			 (if p-resource
			     (cons (car pv)
				   (list (resource-base-prefix p-resource) (cdr pv)))
			     pv))
		       #f)))
	       item-object)))

;; put things in contexts?
(define (item->json-ld item)
  (let* ((resource (item-resource item))
	   (props (resource-properties resource)))
    `((@id . ,(write-uri (expand-uri (item-id item)))) ;; **
      (@type . ,(write-uri (expand-uri (resource-type resource)))) ;; **
      ,@(map (lambda (prop)
	       (cons (car prop)
		     (if (pair? (cdr prop))
			 (list->vector (cdr prop))
			 (cdr prop))))
	     (item-properties item))
      (@context ,@(map (lambda (prop)
			 (cons (car prop)
			       (write-uri (reify (property-predicate prop)))))
		       props)))))

;; to do: handle @contexts
(define (json-ld->item realm json-ld)
  (let ((resource (get-resource-by-uri (alist-ref '@type json-ld)))
	(id (read-uri (alist-ref '@id json-ld))))
    (make-item resource id (get-properties realm resource id))))

