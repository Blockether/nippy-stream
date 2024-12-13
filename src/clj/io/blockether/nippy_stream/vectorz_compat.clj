(ns io.blockether.nippy-stream.vectorz-compat
  (:require
   [clojure.core.matrix :as matrix]
   [taoensso.nippy :as nippy])
  (:import
   [mikera.vectorz Vector Vector1 Vector2 Vector3 Vector4 Vectorz]))

(matrix/set-current-implementation :vectorz)

#_{:clj-kondo/ignore [:unresolved-symbol]}
(nippy/extend-freeze
 Vector
 1
 [^Vector x data-output]
 (nippy/freeze-to-out! data-output (.asDoubleArray x)))

#_{:clj-kondo/ignore [:unresolved-symbol]}
(nippy/extend-thaw
 1
 [data-input]
 (Vector/wrap ^doubles (nippy/thaw-from-in! data-input)))

#_{:clj-kondo/ignore [:unresolved-symbol]}
(nippy/extend-freeze
 Vector1
 2
 [^Vector1 x data-output]
 (nippy/freeze-to-out! data-output (.toDoubleArray x)))


#_{:clj-kondo/ignore [:unresolved-symbol]}
(nippy/extend-thaw
 2
 [data-input]
 (Vectorz/create ^doubles (nippy/thaw-from-in! data-input)))

#_{:clj-kondo/ignore [:unresolved-symbol]}
(nippy/extend-freeze
 Vector2
 3
 [^Vector2 x data-output]
 (nippy/freeze-to-out! data-output (.toDoubleArray x)))


#_{:clj-kondo/ignore [:unresolved-symbol]}
(nippy/extend-thaw
 3
 [data-input]
 (Vectorz/create ^doubles (nippy/thaw-from-in! data-input)))

#_{:clj-kondo/ignore [:unresolved-symbol]}
(nippy/extend-freeze
 Vector3
 4
 [^Vector3 x data-output]
 (nippy/freeze-to-out! data-output (.toDoubleArray x)))

#_{:clj-kondo/ignore [:unresolved-symbol]}
(nippy/extend-thaw
 4
 [data-input]
 (Vectorz/create ^doubles (nippy/thaw-from-in! data-input)))

#_{:clj-kondo/ignore [:unresolved-symbol]}
(nippy/extend-freeze
 Vector4
 5
 [^Vector4 x data-output]
 (nippy/freeze-to-out! data-output (.toDoubleArray x)))

#_{:clj-kondo/ignore [:unresolved-symbol]}
(nippy/extend-thaw
 5
 [data-input]
 (Vectorz/create ^doubles (nippy/thaw-from-in! data-input)))

