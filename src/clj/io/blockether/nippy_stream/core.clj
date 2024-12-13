(ns io.blockether.nippy-stream.core
  #_{:clj-kondo/ignore [:private-call]}
  (:require
   [taoensso.nippy :as nippy  :refer [thaw-from-in!]])
  (:import
   [clojure.lang PersistentVector]
   [io.airlift.compress.lz4 Lz4Compressor Lz4Decompressor]
   [io.blockether.nippy_stream ByteArrayOutputStreamWithReservedSizeMark BytesUtils]
   [java.io
    ByteArrayInputStream
    DataInputStream
    DataOutputStream
    InputStream
    OutputStream]
   [java.nio ByteBuffer]
   [java.util Arrays]
   [java.util.function Consumer Function]
   [java.util.stream LongStream Stream]
   [java.util.zip GZIPOutputStream GZIPInputStream]))

 ;; TODO: Add LZ4 support
 ;; TODO: Add LZ4 lookbehind support
 ;; TODO: Add java.nio.Channel read when applicable
 ;; TODO: Add content negotiation for reading
 ;; TODO: Add replacement for `nippy/thaw` and `nippy/freeze` that can handle streams and is compatible with both nippy and nippy-stream
 ;; TODO: Add tests

(def lz4-compressor (delay (Lz4Compressor.)))

(def lz4-decompressor (delay (Lz4Decompressor.)))

(defn compress
  [^bytes data]
  (let [data-length (int (alength data))
        max-length  (.maxCompressedLength @lz4-compressor data-length)
        ba-max-out  (byte-array max-length)
        actual-compressed (.compress @lz4-compressor
                                     data        0 data-length
                                     ba-max-out  0 max-length)]
    (Arrays/copyOfRange ba-max-out 0 actual-compressed)))

(defn decompress
  [^bytes compressed-data pre-compress-data-length]
  (let [out-len (int pre-compress-data-length)
        ba-max-out (byte-array out-len)]
    (.decompress @lz4-decompressor
                 compressed-data 0 (alength compressed-data)
                 ba-max-out 0 out-len)
    ba-max-out))

(defn- compressed-data->chunk-sizes-looking-behind
  "WIP"
  [^bytes compressed-data
   pre-compress-data-length
   chunk-sizes-data-length]
  (assert
   (<= pre-compress-data-length chunk-sizes-data-length)
   "Unserialized slice length must be less than or equal to the serialized data length")
  (let [out-len (- (int pre-compress-data-length)
                   chunk-sizes-data-length)
        ba-max-out (byte-array out-len)
        ;; We can guess the lookbehind length by doing compress of
        chunk-sizes-start (compress (byte-array chunk-sizes-data-length))]

    (.decompress @lz4-decompressor
                 compressed-data (- (alength compressed-data)
                                    chunk-sizes-start)
                 (alength compressed-data)
                 ba-max-out 0 out-len)
    ba-max-out))

(comment
  (import '[java.util.stream Collectors])

  (def chunks
    (-> (coll->chunks-stream (vec (range 10)) 3)
        (.collect (Collectors/toList))))

  ;; (def serialized-chunks (mapv object->byte-buffer chunks))

  ;; (def chunks-sizes-in-bytes
  ;;   (* Integer/BYTES (count serialized-chunks)))

  (def compressed
    (compress  (byte-array [(byte 0) (byte 1)
                            (byte 0) (byte 0) (byte 2)])))

  (def actual-compressed (alength compressed))

  (decompress
   compressed
   5)

  ;; TODO: Figure out lookbehind ...
  )

;; Nippy Stream HighPerf format for reading and writing large collections
;;                     (!!!! NO COMPRESSION !!!!)
;; -------------------------- PREAMBLE ----------------------------
;;
;; | [NIPPY_STREAM_HIGH_PERF_FORMAT] - to differentiate our format from other formats
;; | [METADATA] - 4 longs
;;    - [long] number of chunks N
;.    - [long] mode
;;      - 0: chunk size interleaved with chunk (GZIP)                                   - :gzip-interleaved
;;      - 1: chunk size interleaved with chunk (LZ4)                                    - :lz4-interleaved
;;      - 2: start with chunks, and when all serialized then end with chunk sizes (LZ4) - :lz4-looked-behind
;;    - [long] reserved
;;    - [long] reserved
;; ------------------------- COMPRESSION -------------------------
;; --------------------------  CHUNK  ----------------------------
;;                       MODE 0 (GZIP) - :gzip-interleaved
;; | [CHUNKS] - N subvecs of original collection  | like (interleave chunks chunk-sizes)
;;
;;                       MODE 1 (LZ4) - :lz4-interleaved  (soon)
;; | [CHUNK SIZES] - N integers (4 bytes each)    | like (interleave chunk-sizes chunks)
;; | [CHUNKS] - N subvecs of original collection  |
;;
;;                       MODE 2 LZ4 - :lz4-looked-behind  (WIP ALPHA)
;; | [CHUNKS] - N subvecs of original collection  | like (concat chunks chunk-sizes)
;; | [CHUNK SIZES] - N integers (4 bytes each)    |
;; --------------------------  CHUNK  ----------------------------

;; - [N int] - bytes (nippy serialized) length of each subvec in bytes


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(def NIPPY_STREAM_HIGH_PERF_FORMAT_MARKER
  (byte-array [0x65 0x64 0x64]))

(def METADATA_LENGTH
  (* Long/BYTES 4))

(def PREAMBLE_BYTES_LENGTH
  (unchecked-add (alength NIPPY_STREAM_HIGH_PERF_FORMAT_MARKER) (long METADATA_LENGTH)))

(defn coll->chunks-stream
  "Splits a given collection into a stream of sub-vectors (chunks) of a specified size.

   **Parameters**

   - coll: The collection to be split (must be a PersistentVector).
   - chunk-size: The desired size of each chunk (must be a positive long).

   **Returns**

   - A Java Stream of sub-vectors, each representing a chunk of the original collection.

   **Example:**

   ```clojure
     (coll->chunks-stream (vec (range 10)) 3) ;; => A stream of unbuffered chunks: [[0 1 2] [3 4 5] [6 7 8] [9]]
   ```"
  [^PersistentVector coll ^long chunk-size]
  (let [coll-size (unchecked-long (count coll))
        chunks   (unchecked-long (Math/ceil (/ (count coll) chunk-size)))]
    (-> (LongStream/range 0 chunks)
        (.boxed)
        (.map (reify Function
                (apply [_ i]
                  (let [start (unchecked-multiply (unchecked-long i) chunk-size)
                        end (min (* (unchecked-inc i) chunk-size) coll-size)]
                    (subvec coll start end))))))))

;; (defn write-coll-as-chunks-to-file!
;;   [^String filename ^PersistentVector coll ^long chunk-size]
;;   (assert (vector? coll) "EDD_HIGH_PERF_FORMAT: Collection must be a vector")
;;   (with-open [file (RandomAccessFile. filename "rw")]
;;     (with-open [file-channel (.getChannel file)]
;;       (let [^Stream chunks-stream (coll->chunks-stream coll chunk-size)
;;             chunks-size (long (Math/ceil (/ (count coll) chunk-size)))
;;             chunks-sizes-bytes-allocation (* chunks-size Integer/BYTES)
;;             header-size (unchecked-add (long PREAMBLE_BYTES_LENGTH) chunks-sizes-bytes-allocation)
;;             ;; Reserved for prepending the size of bytes for each chunk (we know the size only after the serialization)
;;             ^MappedByteBuffer header (.map file-channel FileChannel$MapMode/READ_WRITE 0 header-size)
;;             ^Stream parallel-stream (.parallel chunks-stream)]
;;         (.put header EDD_HIGH_PERF_FORMAT_MARKER)
;;         (.putLong header chunks-size)
;;         (.putLong header 0)
;;         (.putLong header 0)
;;         (.seek file header-size)
;;         (with-open [gzipped-output (DataOutputStream. (GZIPOutputStream. (FileOutputStream. (.getFD file))))]
;;           (-> parallel-stream
;;               (.map (reify Function
;;                       (apply [_ chunk]
;;                              (nippy/fast-freeze chunk))))
;;               (.forEachOrdered
;;                (reify Consumer
;;                  (accept [_ serialized-chunk]
;;                    (.putInt header (alength ^bytes serialized-chunk))
;;                    (.write gzipped-output ^bytes serialized-chunk))))))))))


;; (int->byte-array 200000)

(defn- object->bytes-interleaved
  "Serializes the given object `x` into a byte array for NIPPY_STREAM_HIGH_PERF_FORMAT

  Parameters:
  - x: The object to be serialized.

  Returns:
  - A byte array containing the serialized representation of the object `x`.

   See fast-freeze for more details."
  ([x]
   (object->bytes-interleaved x 2048))
  ([x ^long reserved-size]
   (let [baos (ByteArrayOutputStreamWithReservedSizeMark. (int reserved-size))
         dos  (DataOutputStream. baos)]
     (nippy/-freeze-with-meta! x dos)
     (.syncMarkAfterWrite baos)
     (.toByteArray baos))))

(defn- in->object-interleaved!
  "Deserializes the given byte array `ba` into an object.

  Parameters:
  - in: Input stream containing the serialized object.

  Returns:
  - The deserialized object.

  See fast-thaw for more details."
  [in]
  (let [chunk-bytes-buffer (byte-array Integer/BYTES)
        _ (.read in chunk-bytes-buffer)
        chunk-buffer (byte-array (BytesUtils/fromByteArrayToInt chunk-bytes-buffer))
        _ (.read in chunk-buffer)
        bais (ByteArrayInputStream. chunk-buffer)
        dis  (DataInputStream. bais)]
    (.flush dis)
    (thaw-from-in! dis)))

(defn coll->chunk-size-byte-buffer
  [^PersistentVector coll ^long chunk-size]
  (let [chunks-count (long (Math/ceil (/ (count coll) chunk-size)))
        chunks-sizes-allocation (* chunks-count Integer/BYTES)]
    (ByteBuffer/wrap (byte-array chunks-sizes-allocation))))

(def supported-modes #{:gzip-interleaved :lz4-interleaved :lz4-looked-behind})

(defn coll->info
  [coll ^long chunk-size {:keys [mode]}]
  (assert (contains? supported-modes mode) "NIPPY_STREAM_HIGH_PERF_FORMAT: Unsupported mode")
  (let [coll (if-not (vector? coll) coll (vec coll))
        chunks-count (long (Math/ceil (/ (count coll) chunk-size)))
        chunks-sizes-allocation (* chunks-count Integer/BYTES)
        ^bytes preamble-bytes (byte-array PREAMBLE_BYTES_LENGTH)
        preamble (ByteBuffer/wrap preamble-bytes)
        mode-as-long (case mode
                       :gzip-interleaved 0
                       :lz4-interleaved 1
                       :lz4-looked-behind 2)]
    (.put preamble NIPPY_STREAM_HIGH_PERF_FORMAT_MARKER)
    (.putLong preamble chunks-count)
    (.putLong preamble mode-as-long)
    (.putLong preamble 0) ;; Reserved
    (.putLong preamble 0) ;; Reserved
    {:chunks-count chunks-count
     :chunk-size chunk-size
     :collection coll
     :preamble (.array preamble)
     :chunks-buffer (when (contains? #{:lz4-looked-behind} mode)
                      (ByteBuffer/wrap (byte-array chunks-sizes-allocation)))}))


(defn interleaved-write!
  [^OutputStream out coll-info]
  (let [{:keys [preamble chunk-size collection]} coll-info]
    (.write out ^bytes preamble)
    (with-open [^Stream chunks (coll->chunks-stream collection chunk-size)]
      (-> chunks
          (.onClose (reify Runnable (run [_] (.flush out) (.close out))))
          (.parallel)
          (.map (reify Function (apply [_ chunk] (object->bytes-interleaved chunk))))
          (.sequential)
          (.forEachOrdered (reify Consumer (accept [_ chunk] (.write out ^bytes chunk))))))))

(defn gzip-interleaved-write!
  [^OutputStream out coll-info]
  (with-open [gzipped-output (GZIPOutputStream. out)]
    (interleaved-write! gzipped-output coll-info)))

(gzip-interleaved-write!
 (java.io.FileOutputStream. "edd-format.bin")
 (coll->info (vec (range 10)) 3 {:mode :gzip-interleaved}))

(defn interleaved-read!
  [^InputStream in]
  ;; read everything an remember to add onClose...
  )


    ;;     marker-buffer (byte-array [(.get preamble 0) (.get preamble 1) (.get preamble 2])]
    ;;     _ (assert (java.util.Arrays/equals marker-buffer NIPPY_STREAM_HIGH_PERF_FORMAT_MARKER) "Invalid EDD format marker")
    ;;     chunks-count (.getLong preamble)
    ;;     mode (.getLong preamble)
    ;;     _reserved1 (.getLong preamble)
    ;;     _reserved2 (.getLong preamble)
    ;;     chunks-sizes-allocation (* chunks-count Integer/BYTES)
    ;;     chunks-sizes-bytes (byte-array chunks-sizes-allocation)
    ;;     _! (.read in chunks-sizes-bytes)
    ;;     chunks-sizes-buffer (ByteBuffer/wrap chunks-sizes-bytes)
    ;;     ^IntUnaryOperator buffer-int-mapper (fn [_] (.getInt chunks-sizes-buffer))
    ;;     num-ints (* chunks-count Integer/BYTES)]
    ;; (let [data-stream (GZIPOutputStream. in)]
    ;;   (.read data-stream preamble-bytes)
    ;;   (letfn [(serialize-content [chunk-bytes-allocation]
    ;;             (let [buffer (byte-array chunk-bytes-allocation)]
    ;;               (.read data-stream buffer)
    ;;               (in->object-interleaved! (ByteArrayInputStream. buffer)))]
        ;; (-> (IntStream/range 0 num-ints)
        ;;     (.map buffer-int-mapper)
        ;;     (.boxed)
        ;;     (.map serialize-content)))

(defn gzip-interleaved-read!
  [^InputStream in]
  (interleaved-read! (GZIPInputStream. in)))

*e
;; (gzip-interleaved-read! (java.util.zip.GZIPInputStream. (java.io.FileInputStream. "edd-format.bin")))


*e
*e

;;  (defn stream-to-file!
;;    ([^String filename ^PersistentVector coll ^long chunk-size]
;;     (stream-to-file! filename coll chunk-size {}))
;;    ([^String filename ^PersistentVector coll ^long chunk-size
;;      {:keys [reserved-buffered-size mode]
;;       :or {reserved-buffered-size 1024 mode 0}
;;       :as opts}]
;;     (with-open [file (RandomAccessFile. filename "rw")]
;;       (with-open [file-channel (.getChannel file)]
;;         (let [^Stream chunks-stream (coll->chunks-stream coll chunk-size)
;;               chunks-count (long (Math/ceil (/ (count coll) chunk-size)))
;;               chunks-sizes-allocation (* chunks-count Integer/BYTES)
;;               ^bytes preamble-bytes (byte-array PREAMBLE_BYTES_LENGTH)
;;               preamble (ByteBuffer/wrap preamble-bytes)
;;             ;;  header-size (unchecked-add (long PREAMBLE_BYTES_LENGTH) chunks-sizes-bytes-allocation)
;;               ^Stream parallel-stream (.parallel chunks-stream)]

;;           (.put preamble NIPPY_STREAM_HIGH_PERF_FORMAT_MARKER)
;;           (.put preamble)
;;           (.write file)
;;           (.put header NIPPY_STREAM_HIGH_PERF_FORMAT_MARKER)
;;           (.putLong header chunks-size)
;;           (.putLong header 0)
;;           (.putLong header 0)
;;           (.seek file header-size)
;;           (with-open [gzipped-output (DataOutputStream. (GZIPOutputStream. (FileOutputStream. (.getFD file))))]
;;             (-> parallel-stream
;;                 (.map (reify Function
;;                         (apply [_ chunk]
;;                           (object->byte-buffer chunk reserved-buffered-size))))
;;                 (.forEachOrdered
;;                  (reify Consumer
;;                    (accept [_ serialized-chunk]
;;                      (.putInt header (alength ^bytes serialized-chunk))
;;                      (.write gzipped-output ^bytes serialized-chunk)))))))))))

;; (defn high-perf-file->data
;;   [^String filename]
;;   (let [file (FileInputStream. filename)
;;         channel (.getChannel file)]))
;; ;; (byte-buffer->object (object->byte-buffer [2 2]))

;; (defn write-coll-as-chunks-to-output-stream!
;;   [^OutputStream os ^PersistentVector coll ^long chunk-size]
;;   (assert (vector? coll) "EDD_HIGH_PERF_FORMAT: Collection must be a vector")
;;   (with-open [lz4output (LZ4FrameOutputStream. os)]
;;     (let [^Stream chunks-stream (coll->chunks-stream coll chunk-size)
;;           chunks-size (long (Math/ceil (/ (count coll) chunk-size)))
;;           preamble (ByteBuffer/wrap byte-array (long PREAMBLE_BYTES_LENGTH))]
;;       (doto (ByteBuffer/wrap byte-array (long PREAMBLE_BYTES_LENGTH))
;;         (.put EDD_HIGH_PERF_FORMAT_MARKER)
;;         (.putLong chunks-size)
;;         (.putLong 0)
;;         (.putLong 0)
;;         (.mark)
;;         (.write lz4output ^bytes preamble))

;;       (-> chunks-stream
;;           (.parallelStream)
;;           (.map (fn [chunk] (let [(nippy/fast-freeze chunk)])))
;;           (.sequential)
;;           (.forEachOrdered
;;            (fn [chunk-serialized]
;;              (.write lz4output ^bytes chunk-serialized)
;;              #_(.write lz4output (int->byte-array ^bytes (alength chunk-serialized)))
;;              #_(.putInt preamble (alength ^bytes chunk-serialized))))))))


;; #_(with-open [file (RandomAccessFile. filename "rw")]
;;     (with-open [file-channel (.getChannel file)]
;;       (let [^Stream chunks-stream (coll->chunks-stream coll chunk-size)
;;             chunks-size (long (Math/ceil (/ (count coll) chunk-size)))
;;             chunks-sizes-bytes-allocation (* chunks-size Integer/BYTES)
;;             header-size (unchecked-add (long PREAMBLE_BYTES_LENGTH) chunks-sizes-bytes-allocation)
;;             ;; Reserved for prepending the size of bytes for each chunk (we know the size only after the serialization)
;;             ^ByteBuffer header (ByteBuffer/wrap (byte-array header-size))
;;             output-stream (FileOutputStream. (.getFD file))]

;;         (println chunks-size)
;;         (.put header EDD_HIGH_PERF_FORMAT_MARKER)


;;         (.putLong header chunks-size)
;;         (.putLong header 0)
;;         (.putLong header 0)
;;         (.mark header)

;;         (.write output-stream (.array header))
;;         (.write output-stream (byte-array chunks-sizes-bytes-allocation))

;;         #_(with-open [lz4output (LZ4FrameOutputStream. output-stream)]
;;             (-> chunks-stream
;;                 (.sequential)
;;                 (.forEachOrdered
;;                  (reify Consumer
;;                    (accept [_ chunk]
;;                      (let [serialized-chunk (nippy/fast-freeze chunk)]
;;                        (.write lz4output ^bytes serialized-chunk)
;;                        (.putInt header (int (alength ^bytes serialized-chunk))))))))))))


;; (write-coll-as-chunks-to-file! "edd-format.bin" (vec (repeat 10 (range 10))) 2048)

;; (stream-seq! (read-chunks-from-input-stream->stream (FileInputStream. "edd-format.bin")))

;; (defn read-chunks-from-input-stream->stream
;;   [^InputStream in]
;;   (let [^bytes preamble-bytes (byte-array PREAMBLE_BYTES_LENGTH)
;;         _! (.read in preamble-bytes)
;;         preamble (ByteBuffer/wrap preamble-bytes)
;;         marker-buffer (byte-array [(.get preamble 0) (.get preamble 1) (.get preamble 2)])
;;         ;; _ (println (into [] marker-buffer))
;;         _! (assert (java.util.Arrays/equals marker-buffer EDD_HIGH_PERF_FORMAT_MARKER) "Invalid EDD format marker")
;;         chunks-sizes-count (.getLong preamble)
;;         _ (println chunks-sizes-count)
;;         _reserved1 (.getLong preamble)
;;         _reserved2 (.getLong preamble)
;;         chunk-sizes-bytes (byte-array (* chunks-sizes-count Integer/BYTES))
;;         _! (.read in chunk-sizes-bytes)
;;         chunks-sizes-buffer (ByteBuffer/wrap chunk-sizes-bytes)
;;         ^IntUnaryOperator buffer-int-mapper (fn [_] (.getInt chunks-sizes-buffer))
;;         num-ints (* chunks-sizes-count Integer/BYTES)]
;;     ;; (println num-ints)
;;     (let [data-stream (LZ4FrameInputStream. in)]
;;       (.read data-stream preamble-bytes)
;;       (letfn [(serialize-content [chunk-bytes-allocation]
;;                 (let [buffer (byte-array chunk-bytes-allocation)]
;;                   (println "SIEMA")
;;                   (.read data-stream buffer)
;;                   (nippy/fast-thaw buffer)))]
;;         (-> (IntStream/range 0 num-ints)
;;             (.map buffer-int-mapper)
;;             (.boxed)
;;             (.map serialize-content))))))
