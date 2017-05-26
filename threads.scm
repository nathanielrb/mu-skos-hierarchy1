(use srfi-1 srfi-18 pool http-client)

;(define pool (make-pool (make-list 10)))

(define (pvec #!rest thunks)
  (let* ((len (length thunks))
         (result (make-vector len #f))
         (remaining len)
         (result-mutex (make-mutex))
         (done-mutex (make-mutex)))
    (letrec ((children
              (map (lambda (thunk k)
                     (make-thread
                      (lambda ()
                        (let ((x (thunk)))
                          (mutex-lock! result-mutex #f #f)
                          (vector-set! result k x)
                          (set! remaining (- remaining 1))
                          (mutex-unlock! result-mutex)
                          ;;(thread-terminate! child2)
                          (when (= remaining 0)
                            (mutex-unlock! done-mutex))))))
                   thunks (list-tabulate len values))))
      (mutex-lock! done-mutex #f #f)
      (map thread-start! children)
      (mutex-lock! done-mutex #f #f)
      result)))

(define (take-max lst n)
  (if (or (null? lst) (= n 0))
      '()
      (cons (car lst) (take-max (cdr lst) (- n 1)))))

(define (drop-max lst n)
  (if (or (null? lst) (= n 0))
      lst
      (drop-max (cdr lst) (- n 1))))

(define (pvec-batch batch-size #!rest thunks)
  (let* ((len (length thunks))
         (result (make-vector len #f))
         (remaining len)
         (result-mutex (make-mutex))         
         (done-mutex (make-mutex)))
    (let loop ((thunks thunks))
      (if (null? thunks)
          result
          (letrec ((batch-remaining (min batch-size (length thunks)))
                   (batch-done-mutex (make-mutex))
                   (children
                    (map (lambda (thunk k)
                           (make-thread
                            (lambda ()
                              (let ((x (thunk)))
                                (mutex-lock! result-mutex #f #f)
                                (vector-set! result k x)
                                (set! remaining (- remaining 1))
                                (set! batch-remaining (- batch-remaining 1))
                                (mutex-unlock! result-mutex)
                                (when (= batch-remaining 0)
                                  (mutex-unlock! batch-done-mutex)) ))))
                         (take-max thunks batch-size) (list-tabulate len values))))
            (mutex-lock! batch-done-mutex #f #f)
            (map thread-start! children)
            (mutex-lock! batch-done-mutex #f #F)
            (loop (drop-max thunks batch-size)))))))


(define (pmap fn #!rest lists)
  (vector->list
   (apply pvec
    (apply map
           (lambda (e) (lambda () (fn e)))
           lists))))

(define (pmap-batch batch-size fn #!rest lists)
  (vector->list
   (apply pvec-batch batch-size
     (apply map
            (lambda (e) (lambda () (fn e)))
            lists))))

(define (pmap-vec fn #!rest lists)
   (apply pvec
    (apply map
           (lambda (e) (lambda () (fn e)))
           lists)))

(define pool (make-pool '(1 2 3 4 5)))

(define t (lambda () 
                (query-with-vars (x y) "SELECT * WHERE { <http://data.europa.eu/eurostat/id/taxonomy/ECOICOP/concept/041220> ?p ?o } LIMIT 1" (list x y))))


(define u (lambda () 
            (call-with-value-from-pool pool
              (lambda (_)
                (query-with-vars (x y) "SELECT * WHERE { <http://data.europa.eu/eurostat/id/taxonomy/ECOICOP/concept/041220> ?p ?o } LIMIT 1" (list x y))))))

(define v (lambda () 
                (sparql/select2
                 "SELECT * WHERE { <http://data.europa.eu/eurostat/id/taxonomy/ECOICOP/concept/041220> ?p ?o } LIMIT 1")))
