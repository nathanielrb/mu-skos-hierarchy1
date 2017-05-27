(define (assoc-get field object)
  (cdr (assoc field object)))

(define (last-substr? str substr)
  (substring=? str substr
	       (- (string-length str)
		  (string-length substr))))

(define (conc-last str substr)
  (if (last-substr? str substr)
      str
      (conc str substr)))

(define (assoc-val key alist)
  (let ((v (assoc key alist)))
    (and v (cdr v))))

(define (cdr-when p)
  (and (pair? p) (cdr p)))

(define (car-when p)
  (and (pair? p) (car p)))
