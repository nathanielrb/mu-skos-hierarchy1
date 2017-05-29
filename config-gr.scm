(load "graph-resources.scm")

(development-mode? #t)

(debug-file "./debug.log")

(*print-queries?* #t)

(*default-graph* '<http://mu.semte.ch/graph-resources/>)

(*sparql-endpoint* "http://127.0.0.1:8890/sparql?")

(define-namespace mu "http://mu.semte.ch/application/")

(define-namespace gd "http://mu.semte.ch/graph-resources/")

(define-namespace eurostat "http://mu.semte.ch/eurostat/")

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
			    (properties (gtin (predicate (mu "gtin")))
					(amount (predicate (mu "amount")))
					(description (predicate (mu "description"))
						     (multiple? #t))
					(ecoicop (predicate (mu "class"))
						 (resource class)))))

(define-resource 'class `((type . (eurostat ECOICOP))
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
