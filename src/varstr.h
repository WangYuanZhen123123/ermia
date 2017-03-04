#pragma once

#include "macros.h"

#ifdef MASSTREE
#include "prefetch.h"
#include "masstree/string_slice.hh"
#endif

struct varstr {
  friend std::ostream &operator<<(std::ostream &o, const varstr &k);
public:
  inline varstr() : l(0), p(NULL) {}

  inline varstr(const uint8_t *p, uint32_t l) : l(l), p(p) {}
  inline varstr(const char *p, uint32_t l) : l(l), p((const uint8_t *)p) {}
  inline void copy_from(const uint8_t *s, uint32_t l) { copy_from((char *)s, l); }
  inline void copy_from(const varstr* v) { copy_from(v->p, v->l); }

  inline void copy_from(const char *s, uint32_t ll) {
    if(ll) {
      memcpy((void*)p, s, ll);
    }
    l = ll;
  }

  inline bool operator==(const varstr &that) const {
    if (size() != that.size())
      return false;
    return memcmp(data(), that.data(), size()) == 0;
  }

  inline varstr& operator=(const varstr& other) {
    p = other.p;
    l = other.l;
    return *this;
  }

  inline bool operator!=(const varstr &that) const { return !operator==(that); }

  inline bool operator<(const varstr &that) const {
    int r = memcmp(data(), that.data(), std::min(size(), that.size()));
    return r < 0 || (r == 0 && size() < that.size());
  }

  inline bool operator>=(const varstr &that) const { return !operator<(that); }

  inline bool operator<=(const varstr &that) const {
    int r = memcmp(data(), that.data(), std::min(size(), that.size()));
    return r < 0 || (r == 0 && size() <= that.size());
  }

  inline bool operator>(const varstr &that) const { return !operator<=(that); }

  inline int compare(const varstr &that) const {
    return memcmp(data(), that.data(), std::min(size(), that.size()));
  }

  inline uint64_t slice() const {
    uint64_t ret = 0;
    uint8_t *rp = (uint8_t *) &ret;
    for (uint32_t i = 0; i < std::min(l, uint32_t(8)); i++)
      rp[i] = p[i];
    return util::host_endian_trfm<uint64_t>()(ret);
  }

#ifdef MASSTREE
  inline uint64_t slice_at(int pos) const {
    return string_slice<uint64_t>::make_comparable((const char*) p + pos, std::min(int(l - pos), 8));
  }
#endif

  inline varstr shift() const {
    ASSERT(l >= 8);
    return varstr(p + 8, l - 8);
  }

  inline varstr shift_many(uint32_t n) const {
    ASSERT(l >= 8 * n);
    return varstr(p + 8 * n, l - 8 * n);
  }

  inline uint32_t size() const { return l; }
  inline const uint8_t* data() const { return p; }
  inline uint8_t* data() { return (uint8_t *)p; }
  inline bool empty() const { return not size(); }

#ifdef MASSTREE
  inline operator lcdf::Str() const {
    return lcdf::Str(p, l);
  }
#endif

  uint32_t l;
  const uint8_t *p; // must be the last field
};
