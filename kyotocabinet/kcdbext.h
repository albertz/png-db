/*************************************************************************************************
 * Database extension
 *                                                               Copyright (C) 2009-2011 FAL Labs
 * This file is part of Kyoto Cabinet.
 * This program is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either version
 * 3 of the License, or any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with this program.
 * If not, see <http://www.gnu.org/licenses/>.
 *************************************************************************************************/


#ifndef _KCDBEXT_H                       // duplication check
#define _KCDBEXT_H

#include <kccommon.h>
#include <kcutil.h>
#include <kcthread.h>
#include <kcfile.h>
#include <kccompress.h>
#include <kccompare.h>
#include <kcmap.h>
#include <kcregex.h>
#include <kcdb.h>
#include <kcplantdb.h>
#include <kcprotodb.h>
#include <kcstashdb.h>
#include <kccachedb.h>
#include <kchashdb.h>
#include <kcdirdb.h>
#include <kcpolydb.h>

namespace kyotocabinet {                 // common namespace


/**
 * MapReduce framework.
 * @note Although this framework is not distributed or concurrent, it is useful for aggregate
 * calculation with less CPU loading and less memory usage.
 */
class MapReduce {
 public:
  class MapEmitter;
  class ValueIterator;
 private:
  class MapVisitor;
  struct MergeLine;
  /** An alias of vector of loaded values. */
  typedef std::vector<std::string> Values;
  /** The default number of temporary databases. */
  static const size_t MRDEFDBNUM = 8;
  /** The maxinum number of temporary databases. */
  static const size_t MRMAXDBNUM = 256;
  /** The default cache limit. */
  static const int64_t MRDEFCLIM = 512LL << 20;
  /** The default cache bucket numer. */
  static const int64_t MRDEFCBNUM = 1048583LL;
  /** The bucket number of temprary databases. */
  static const int64_t MRDBBNUM = 512LL << 10;
  /** The page size of temprary databases. */
  static const int32_t MRDBPSIZ = 32768;
  /** The mapped size of temprary databases. */
  static const int64_t MRDBMSIZ = 516LL * 4096;
  /** The page cache capacity of temprary databases. */
  static const int64_t MRDBPCCAP = 16LL << 20;
 public:
  /**
   * Data emitter for the mapper.
   */
  class MapEmitter {
    friend class MapReduce;
    friend class MapReduce::MapVisitor;
   public:
    /**
     * Emit a record from the mapper.
     * @param kbuf the pointer to the key region.
     * @param ksiz the size of the key region.
     * @param vbuf the pointer to the value region.
     * @param vsiz the size of the value region.
     * @return true on success, or false on failure.
     */
    bool emit(const char* kbuf, size_t ksiz, const char* vbuf, size_t vsiz) {
      _assert_(kbuf && ksiz <= MEMMAXSIZ && vbuf && vsiz <= MEMMAXSIZ);
      bool err = false;
      size_t rsiz = sizevarnum(vsiz) + vsiz;
      char stack[NUMBUFSIZ*4];
      char* rbuf = rsiz > sizeof(stack) ? new char[rsiz] : stack;
      char* wp = rbuf;
      wp += writevarnum(rbuf, vsiz);
      std::memcpy(wp, vbuf, vsiz);
      mr_->cache_->append(kbuf, ksiz, rbuf, rsiz);
      if (rbuf != stack) delete[] rbuf;
      mr_->csiz_ += rsiz;
      return !err;
    }
   private:
    /**
     * Default constructor.
     */
    explicit MapEmitter(MapReduce* mr) : mr_(mr) {
      _assert_(true);
    }
    /**
     * Destructor.
     */
    ~MapEmitter() {
      _assert_(true);
    }
    /** Dummy constructor to forbid the use. */
    MapEmitter(const MapEmitter&);
    /** Dummy Operator to forbid the use. */
    MapEmitter& operator =(const MapEmitter&);
    /** The owner object. */
    MapReduce* mr_;
  };
  /**
   * Value iterator for the reducer.
   */
  class ValueIterator {
    friend class MapReduce;
   public:
    /**
     * Get the next value.
     * @param sp the pointer to the variable into which the size of the region of the return
     * value is assigned.
     * @return the pointer to the next value region, or NULL if no value remains.
     */
    const char* next(size_t* sp) {
      _assert_(sp);
      if (!vptr_) {
        if (vit_ == vend_) return NULL;
        vptr_ = vit_->data();
        vsiz_ = vit_->size();
        vit_++;
      }
      uint64_t vsiz;
      size_t step = readvarnum(vptr_, vsiz_, &vsiz);
      vptr_ += step;
      vsiz_ -= step;
      const char* vbuf = vptr_;
      *sp = vsiz;
      vptr_ += vsiz;
      vsiz_ -= vsiz;
      if (vsiz_ < 1) vptr_ = NULL;
      return vbuf;
    }
   private:
    /**
     * Default constructor.
     */
    explicit ValueIterator(Values::const_iterator vit, Values::const_iterator vend) :
        vit_(vit), vend_(vend), vptr_(NULL), vsiz_(0) {
      _assert_(true);
    }
    /**
     * Destructor.
     */
    ~ValueIterator() {
      _assert_(true);
    }
    /** Dummy constructor to forbid the use. */
    ValueIterator(const ValueIterator&);
    /** Dummy Operator to forbid the use. */
    ValueIterator& operator =(const ValueIterator&);
    /** The current iterator of loaded values. */
    Values::const_iterator vit_;
    /** The ending iterator of loaded values. */
    Values::const_iterator vend_;
    /** The pointer of the current value. */
    const char* vptr_;
    /** The size of the current value. */
    size_t vsiz_;
  };
  /**
   * Execution options.
   */
  enum Option {
    XNOLOCK = 1 << 0,                    ///< avoid locking against update operations
    XNOCOMP = 1 << 1                     ///< avoid compression of temporary databases
  };
  /**
   * Default constructor.
   */
  explicit MapReduce() :
      rcomp_(NULL), tmpdbs_(NULL), dbnum_(MRDEFDBNUM), dbclock_(0), keyclock_(0),
      cache_(NULL), csiz_(0), clim_(MRDEFCLIM), cbnum_(MRDEFCBNUM) {
    _assert_(true);
  }
  /**
   * Destructor.
   */
  virtual ~MapReduce() {
    _assert_(true);
  }
  /**
   * Map a record data.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param vbuf the pointer to the value region.
   * @param vsiz the size of the value region.
   * @param emitter the emitter object.
   * @return true on success, or false on failure.
   * @note To avoid deadlock, any explicit database operation must not be performed in this
   * function.
   */
  virtual bool map(const char* kbuf, size_t ksiz, const char* vbuf, size_t vsiz,
                   MapEmitter* emitter) = 0;
  /**
   * Reduce a record data.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param iter the iterator to get the values.
   * @return true on success, or false on failure.
   * @note To avoid deadlock, any explicit database operation must not be performed in this
   * function.
   */
  virtual bool reduce(const char* kbuf, size_t ksiz, ValueIterator* iter) = 0;
  /**
   * Preprocess the map operations.
   * @return true on success, or false on failure.
   */
  virtual bool preprocess() {
    _assert_(true);
    return true;
  }
  /**
   * Mediate between the map and the reduce phases.
   * @return true on success, or false on failure.
   */
  virtual bool midprocess() {
    _assert_(true);
    return true;
  }
  /**
   * Postprocess the reduce operations.
   * @return true on success, or false on failure.
   */
  virtual bool postprocess() {
    _assert_(true);
    return true;
  }
  /**
   * Process a log message.
   * @param name the name of the event.
   * @param message a supplement message.
   * @return true on success, or false on failure.
   */
  virtual bool log(const char* name, const char* message) {
    _assert_(name && message);
    return true;
  }
  /**
   * Execute the MapReduce process about a database.
   * @param db the source database.
   * @param tmppath the path of a directory for the temporary data storage.  If it is an empty
   * string, temporary data are handled on memory.
   * @param opts the optional features by bitwise-or: MapReduce::XNOLOCK to avoid locking
   * against update operations by other threads, MapReduce::XNOCOMP to avoid compression of
   * temporary databases.
   * @return true on success, or false on failure.
   */
  bool execute(BasicDB* db, const std::string& tmppath = "", uint32_t opts = 0) {
    int64_t count = db->count();
    if (count < 0) return false;
    bool err = false;
    double stime, etime;
    rcomp_ = LEXICALCOMP;
    BasicDB* idb = db;
    if (typeid(*db) == typeid(PolyDB)) {
      PolyDB* pdb = (PolyDB*)idb;
      idb = pdb->reveal_inner_db();
    }
    const std::type_info& info = typeid(*idb);
    if (info == typeid(GrassDB)) {
      GrassDB* gdb = (GrassDB*)idb;
      rcomp_ = gdb->rcomp();
    } else if (info == typeid(TreeDB)) {
      TreeDB* tdb = (TreeDB*)idb;
      rcomp_ = tdb->rcomp();
    } else if (info == typeid(ForestDB)) {
      ForestDB* fdb = (ForestDB*)idb;
      rcomp_ = fdb->rcomp();
    }
    tmpdbs_ = new BasicDB*[dbnum_];
    if (tmppath.empty()) {
      if (!logf("prepare", "started to open temporary databases on memory")) err = true;
      stime = time();
      for (size_t i = 0; i < dbnum_; i++) {
        GrassDB* gdb = new GrassDB;
        int32_t myopts = 0;
        if (!(opts & XNOCOMP)) myopts |= GrassDB::TCOMPRESS;
        gdb->tune_options(myopts);
        gdb->tune_buckets(MRDBBNUM / 2);
        gdb->tune_page(MRDBPSIZ);
        gdb->tune_page_cache(MRDBPCCAP);
        gdb->tune_comparator(rcomp_);
        gdb->open("%", GrassDB::OWRITER | GrassDB::OCREATE | GrassDB::OTRUNCATE);
        tmpdbs_[i] = gdb;
      }
      etime = time();
      if (!logf("prepare", "opening temporary databases finished: time=%.6f", etime - stime))
        err = true;
      if (err) {
        delete[] tmpdbs_;
        return false;
      }
    } else {
      File::Status sbuf;
      if (!File::status(tmppath, &sbuf) || !sbuf.isdir) {
        db->set_error(_KCCODELINE_, BasicDB::Error::NOREPOS, "no such directory");
        delete[] tmpdbs_;
        return false;
      }
      if (!logf("prepare", "started to open temporary databases under %s", tmppath.c_str()))
        err = true;
      stime = time();
      uint32_t pid = getpid() & UINT16MAX;
      uint32_t tid = Thread::hash() & UINT16MAX;
      uint32_t ts = time() * 1000;
      for (size_t i = 0; i < dbnum_; i++) {
        std::string childpath =
            strprintf("%s%cmr-%04x-%04x-%08x-%03d%ckct",
                      tmppath.c_str(), File::PATHCHR, pid, tid, ts, (int)(i + 1), File::EXTCHR);
        TreeDB* tdb = new TreeDB;
        int32_t myopts = TreeDB::TSMALL | TreeDB::TLINEAR;
        if (!(opts & XNOCOMP)) myopts |= TreeDB::TCOMPRESS;
        tdb->tune_options(myopts);
        tdb->tune_buckets(MRDBBNUM);
        tdb->tune_page(MRDBPSIZ);
        tdb->tune_map(MRDBMSIZ);
        tdb->tune_page_cache(MRDBPCCAP);
        tdb->tune_comparator(rcomp_);
        if (!tdb->open(childpath, TreeDB::OWRITER | TreeDB::OCREATE | TreeDB::OTRUNCATE)) {
          const BasicDB::Error& e = tdb->error();
          db->set_error(_KCCODELINE_, e.code(), e.message());
          err = true;
        }
        tmpdbs_[i] = tdb;
      }
      etime = time();
      if (!logf("prepare", "opening temporary databases finished: time=%.6f", etime - stime))
        err = true;
      if (err) {
        for (size_t i = 0; i < dbnum_; i++) {
          delete tmpdbs_[i];
        }
        delete[] tmpdbs_;
        return false;
      }
    }
    if (opts & XNOLOCK) {
      MapChecker mapchecker;
      MapVisitor mapvisitor(this, &mapchecker, db->count());
      mapvisitor.visit_before();
      if (!err) {
        BasicDB::Cursor* cur = db->cursor();
        if (!cur->jump()) err = true;
        while (!err) {
          if (!cur->accept(&mapvisitor, false, true)) {
            if (cur->error() != BasicDB::Error::NOREC) err = true;
            break;
          }
        }
        delete cur;
      }
      if (mapvisitor.error()) err = true;
      mapvisitor.visit_after();
    } else {
      MapChecker mapchecker;
      MapVisitor mapvisitor(this, &mapchecker, db->count());
      if (!err && !db->iterate(&mapvisitor, false, &mapchecker)) err = true;
      if (mapvisitor.error()) err = true;
    }
    if (!logf("clean", "closing the temporary databases")) err = true;
    stime = time();
    for (size_t i = 0; i < dbnum_; i++) {
      assert(tmpdbs_[i]);
      std::string path = tmpdbs_[i]->path();
      if (!tmpdbs_[i]->clear()) {
        const BasicDB::Error& e = tmpdbs_[i]->error();
        db->set_error(_KCCODELINE_, e.code(), e.message());
        err = true;
      }
      if (!tmpdbs_[i]->close()) {
        const BasicDB::Error& e = tmpdbs_[i]->error();
        db->set_error(_KCCODELINE_, e.code(), e.message());
        err = true;
      }
      if (!tmppath.empty()) File::remove(path);
      delete tmpdbs_[i];
    }
    etime = time();
    if (!logf("clean", "closing the temporary databases finished: time=%.6f",
              etime - stime)) err = true;
    delete[] tmpdbs_;
    return !err;
  }
  /**
   * Set the storage configurations.
   * @param dbnum the number of temporary databases.
   * @param clim the limit size of the internal cache.
   * @param cbnum the bucket number of the internal cache.
   */
  void tune_storage(int32_t dbnum, int64_t clim, int64_t cbnum) {
    _assert_(true);
    dbnum_ = dbnum > 0 ? dbnum : MRDEFDBNUM;
    if (dbnum_ > MRMAXDBNUM) dbnum_ = MRMAXDBNUM;
    clim_ = clim > 0 ? clim : MRDEFCLIM;
    cbnum_ = cbnum > 0 ? cbnum : MRDEFCBNUM;
    if (cbnum_ > INT16MAX) cbnum_ = nearbyprime(cbnum_);
  }
 private:
  /**
   * Checker for the map process.
   */
  class MapChecker : public BasicDB::ProgressChecker {
   public:
    /** constructor */
    explicit MapChecker() : stop_(false) {}
    /** stop the process */
    void stop() {
      stop_ = true;
    }
    /** check whether stopped */
    bool stopped() {
      return stop_;
    }
   private:
    /** check whether stopped */
    bool check(const char* name, const char* message, int64_t curcnt, int64_t allcnt) {
      return !stop_;
    }
    bool stop_;                          ///< flag for stop
  };
  /**
   * Visitor for the map process.
   */
  class MapVisitor : public BasicDB::Visitor {
   public:
    /** constructor */
    explicit MapVisitor(MapReduce* mr, MapChecker* checker, int64_t scale) :
        mr_(mr), checker_(checker), emitter_(mr), scale_(scale),
        stime_(0), err_(false) {}
    /** get the error flag */
    bool error() {
      return err_;
    }
    /** preprocess the mappter */
    void visit_before() {
      if (!mr_->preprocess()) err_ = true;
      stime_ = time();
      mr_->dbclock_ = 0;
      mr_->keyclock_ = 0;
      mr_->cache_ = new TinyHashMap(mr_->cbnum_);
      mr_->csiz_ = 0;
      if (!mr_->logf("map", "started the map process: scale=%lld", (long long)scale_))
        err_ = true;
    }
    /** postprocess the mappter and call the reducer */
    void visit_after() {
      if (mr_->cache_->count() > 0 && !mr_->flush_cache()) err_ = true;
      delete mr_->cache_;
      if (!mr_->midprocess()) err_ = true;
      double etime = time();
      if (!mr_->logf("map", "the map process finished: time=%.6f", etime - stime_))
        err_ = true;
      if (!err_ && !mr_->execute_reduce()) err_ = true;
      if (!mr_->postprocess()) err_ = true;
    }
   private:
    /** visit a record */
    const char* visit_full(const char* kbuf, size_t ksiz,
                           const char* vbuf, size_t vsiz, size_t* sp) {
      if (!mr_->map(kbuf, ksiz, vbuf, vsiz, &emitter_)) {
        checker_->stop();
        err_ = true;
      }
      if (mr_->csiz_ >= mr_->clim_ && !mr_->flush_cache()) {
        checker_->stop();
        err_ = true;
      }
      return NOP;
    }
    MapReduce* mr_;                      ///< driver
    MapChecker* checker_;                ///< checker
    MapEmitter emitter_;                 ///< emitter
    int64_t scale_;                      ///< number of records
    double stime_;                       ///< start time
    bool err_;                           ///< error flag
  };
  /**
   * Front line of a merging list.
   */
  struct MergeLine {
    BasicDB::Cursor* cur;                ///< cursor
    Comparator* rcomp;                   ///< record comparator
    char* kbuf;                          ///< pointer to the key
    size_t ksiz;                         ///< size of the key
    const char* vbuf;                    ///< pointer to the value
    size_t vsiz;                         ///< size of the value
    /** comparing operator */
    bool operator <(const MergeLine& right) const {
      return rcomp->compare(kbuf, ksiz, right.kbuf, right.ksiz) > 0;
    }
  };
  /**
   * Process a log message.
   * @param name the name of the event.
   * @param format the printf-like format string.
   * @param ... used according to the format string.
   * @return true on success, or false on failure.
   */
  bool logf(const char* name, const char* format, ...) {
    _assert_(name && format);
    va_list ap;
    va_start(ap, format);
    std::string message;
    vstrprintf(&message, format, ap);
    va_end(ap);
    return log(name, message.c_str());
  }
  /**
   * Flush all cache records.
   * @return true on success, or false on failure.
   */
  bool flush_cache() {
    _assert_(true);
    bool err = false;
    if (!logf("map", "started to flushing the cache: count=%lld size=%lld",
              (long long)cache_->count(), (long long)csiz_)) err = true;
    double stime = time();
    BasicDB* db = tmpdbs_[dbclock_];
    TinyHashMap::Sorter sorter(cache_);
    const char* kbuf, *vbuf;
    size_t ksiz, vsiz;
    while ((kbuf = sorter.get(&ksiz, &vbuf, &vsiz)) != NULL) {
      if (!db->append(kbuf, ksiz, vbuf, vsiz)) err = true;
      sorter.step();
    }
    cache_->clear();
    csiz_ = 0;
    dbclock_ = (dbclock_ + 1) % dbnum_;
    double etime = time();
    if (!logf("map", "flushing the cache finished: time=%.6f", etime - stime)) err = true;
    return !err;
  }
  /**
   * Execute the reduce part.
   * @return true on success, or false on failure.
   */
  bool execute_reduce() {
    bool err = false;
    int64_t scale = 0;
    for (size_t i = 0; i < dbnum_; i++) {
      scale += tmpdbs_[i]->count();
    }
    if (!logf("reduce", "started the reduce process: scale=%lld", (long long)scale)) err = true;
    double stime = time();
    std::priority_queue<MergeLine> lines;
    for (size_t i = 0; i < dbnum_; i++) {
      MergeLine line;
      line.cur = tmpdbs_[i]->cursor();
      line.rcomp = rcomp_;
      line.cur->jump();
      line.kbuf = line.cur->get(&line.ksiz, &line.vbuf, &line.vsiz, true);
      if (line.kbuf) {
        lines.push(line);
      } else {
        delete line.cur;
      }
    }
    char* lkbuf = NULL;
    size_t lksiz = 0;
    Values values;
    while (!err && !lines.empty()) {
      MergeLine line = lines.top();
      lines.pop();
      if (lkbuf && (lksiz != line.ksiz || std::memcmp(lkbuf, line.kbuf, lksiz))) {
        if (!call_reducer(lkbuf, lksiz, values)) err = true;
        values.clear();
      }
      values.push_back(std::string(line.vbuf, line.vsiz));
      delete[] lkbuf;
      lkbuf = line.kbuf;
      lksiz = line.ksiz;
      line.kbuf = line.cur->get(&line.ksiz, &line.vbuf, &line.vsiz, true);
      if (line.kbuf) {
        lines.push(line);
      } else {
        delete line.cur;
      }
    }
    if (lkbuf) {
      if (!err && !call_reducer(lkbuf, lksiz, values)) err = true;
      values.clear();
      delete[] lkbuf;
    }
    while (!lines.empty()) {
      MergeLine line = lines.top();
      lines.pop();
      delete[] line.kbuf;
      delete line.cur;
    }
    double etime = time();
    if (!logf("reduce", "the reduce process finished: time=%.6f", etime - stime)) err = true;
    return !err;
  }
  /**
   * Call the reducer.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param values a vector of the values.
   * @return true on success, or false on failure.
   */
  bool call_reducer(const char* kbuf, size_t ksiz, const Values& values) {
    _assert_(kbuf && ksiz <= MEMMAXSIZ);
    bool err = false;
    ValueIterator iter(values.begin(), values.end());
    if (!reduce(kbuf, ksiz, &iter)) err = true;
    return !err;
  }
  /** Dummy constructor to forbid the use. */
  MapReduce(const MapReduce&);
  /** Dummy Operator to forbid the use. */
  MapReduce& operator =(const MapReduce&);
  /** The record comparator. */
  Comparator* rcomp_;
  /** The temporary databases. */
  BasicDB** tmpdbs_;
  /** The number of temporary databases. */
  size_t dbnum_;
  /** The logical clock for temporary databases. */
  int64_t dbclock_;
  /** The logical clock for keys. */
  int64_t keyclock_;
  /** The cache for emitter. */
  TinyHashMap* cache_;
  /** The current size of the cache for emitter. */
  int64_t csiz_;
  /** The limit size of the cache for emitter. */
  int64_t clim_;
  /** The bucket number of the cache for emitter. */
  int64_t cbnum_;
};


}                                        // common namespace

#endif                                   // duplication check

// END OF FILE
