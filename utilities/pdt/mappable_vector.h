//
// Created by Dim Dew on 2020-07-21.
//

#ifndef PATH_DECOMPOSITION_TRIE_MAPPABLE_VECTOR_H
#define PATH_DECOMPOSITION_TRIE_MAPPABLE_VECTOR_H

#include <functional>
#include <cstring>
#include <string>

namespace rocksdb {
namespace succinct {
    // sbh add
    struct EncodeArgs {
        size_t size;
        std::string* dst;
        EncodeArgs(std::string* buf) : size(0), dst(buf) {}
    };

    struct DecodeArgs {
        size_t size;
        const char *src;
        DecodeArgs(const char *buf) : size(0), src(buf) {}
    };

    template <typename T> 
    static inline void EncodeType(EncodeArgs *arg, T value)
    {
        arg->dst->append((const char *)&value, sizeof(T));
        arg->size += sizeof(T);
    }

    template <typename T> 
    static inline void DecodeType(DecodeArgs *arg, T &value)
    {
        memcpy(&value, arg->src, sizeof(T));
        arg->src += sizeof(T);
        arg->size += sizeof(T);
    }

    static inline void EncodeNone(EncodeArgs *arg, size_t n) {
        char buf[n+1];
        arg->dst->append((const char *)buf, n);
        arg->size += n;
    }

    static inline void DecodeNone(DecodeArgs *arg, size_t n)
    {
        arg->src += n;
        arg->size += n;
    }

    typedef std::function<void()> deleter_t;

    template <typename T>
    class mappable_vector {
    public:
        typedef T value_type;
        typedef const T* iterator;
        typedef const T* const_iterator;

        mappable_vector()
                : m_data(0)
                , m_size(0)
                , m_deleter(nullptr)
        {}

        mappable_vector(const T* data, uint64_t word_size)
                : m_data(data)
                , m_size(word_size)
                , m_deleter(nullptr)
        {}

        mappable_vector(const std::vector<T>& from)
                : m_data(0)
                , m_size(0)
        {
            size_t size = from.size();
            T* data = new T[size];
            m_deleter = [data] {
                delete[] data;
            };

            std::copy(std::begin(from),
                      std::end(from),
                      data);
            m_data = data;
            m_size = size;
        }

        ~mappable_vector() {
            if (m_deleter) {
                m_deleter();
            }
        }

        void swap(mappable_vector& other) {
            using std::swap;
            swap(m_data, other.m_data);
            swap(m_size, other.m_size);
            swap(m_deleter, other.m_deleter);
        }

        // sbh add
        void Encode(EncodeArgs *arg) {
            EncodeType(arg, m_size);
            if(m_size == 0) return;

            char buf[m_size * sizeof(T) + 1];
            memcpy(buf, m_data, m_size * sizeof(T));
            arg->dst->append(buf, m_size * sizeof(T));
            // std::cout << "Encode: (" << m_size << ")" << std::endl;
        }

        void Decode(DecodeArgs *arg) {
            DecodeType(arg, m_size);
            if(m_size == 0) return;
            // std::cout << "Decode: (" << m_size << ")" << std::endl;
            if(m_data && m_deleter) m_deleter();

        #ifdef REUSE_DECODE_BUF
            m_deleter = 0;
            m_data = (T *)(arg->src);
        #else
            if(m_data && m_deleter) m_deleter();

            T* data = new T[m_size];
            m_deleter = [data] { delete[] data; };
            std::copy((T *)(arg->src), ((T *)(arg->src)) + m_size, data);
            m_data = data;
        #endif
            arg->src += m_size * sizeof(T);
        }

        void clear() {
            mappable_vector().swap(*this);
        }

        void steal(std::vector<T>& vec) {
            clear();
            m_size = vec.size();
            if (m_size) {
                std::vector<T>* new_vec = new std::vector<T>;
                new_vec->swap(vec);
                m_deleter = [new_vec] {
                    delete new_vec;
                };
                m_data = &(*new_vec)[0];
            }
        }

        void assign(const std::vector<T>& from) {
            clear();
            mappable_vector(from).swap(*this);
        }

        uint64_t size() const {
            return m_size;
        }

        inline const_iterator begin() const {
            return m_data;
        }

        inline const_iterator end() const {
            return m_data + m_size;
        }

        inline T const& operator[](uint64_t i) const {
            assert(i < m_size);
            return m_data[i];
        }

        inline T const* data() const {
            return m_data;
        }

        inline T const& back() const {
            return (*this)[m_size - 1];
        }

//        inline void prefetch(size_t i) const {
//            succinct::intrinsics::prefetch(m_data + i);
//        }

//        friend class detail::freeze_visitor;
//        friend class detail::map_visitor;
//        friend class detail::sizeof_visitor;

    protected:
        const T* m_data;
        uint64_t m_size;        // word size
        deleter_t m_deleter;
    };
}
}

#endif //PATH_DECOMPOSITION_TRIE_MAPPABLE_VECTOR_H
