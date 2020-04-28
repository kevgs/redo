#include <atomic>
#include <mutex>
#include <thread>

#include <cassert>
#include <cstddef>

#ifdef _WIN32
#include <windows.h>
#endif

#ifdef __linux__
#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

#include <immintrin.h>

namespace redo {

struct group_commit_waiter_t;

class group_commit_lock {
  using value_type = size_t;
  std::mutex m_mtx;
  std::atomic<value_type> m_value;
  std::atomic<value_type> m_pending_value;
  bool m_lock;
  group_commit_waiter_t *m_waiters_list;

public:
  group_commit_lock();
  enum lock_return_code { ACQUIRED, EXPIRED };
  lock_return_code acquire(value_type num);
  void release(value_type num);
  value_type value() const;
  value_type pending() const;
  void set_pending(value_type num);
};

class binary_semaphore {
public:
  /**Wait until semaphore becomes signalled, and atomically reset the state
  to non-signalled*/
  void wait();
  /** signals the semaphore */
  void wake();

private:
#if defined(__linux__) || defined(_WIN32)
  std::atomic<int> m_signalled;
  const std::memory_order mem_order = std::memory_order::memory_order_acq_rel;

public:
  binary_semaphore() : m_signalled(0) {}
#else
  std::mutex m_mtx{};
  std::condition_variable m_cv{};
  bool m_signalled = false;
#endif
};

#if defined(__linux__) || defined(_WIN32)
void binary_semaphore::wait() {
  for (;;) {
    if (m_signalled.exchange(0, mem_order) == 1) {
      break;
    }
#ifdef _WIN32
    int zero = 0;
    WaitOnAddress(&m_signalled, &zero, sizeof(m_signalled), INFINITE);
#else
    syscall(SYS_futex, &m_signalled, FUTEX_WAIT_PRIVATE, 0, NULL, NULL, 0);
#endif
  }
}

void binary_semaphore::wake() {
  if (m_signalled.exchange(1, mem_order) == 0) {
#ifdef _WIN32
    WakeByAddressSingle(&m_signalled);
#else
    syscall(SYS_futex, &m_signalled, FUTEX_WAKE_PRIVATE, 1, NULL, NULL, 0);
#endif
  }
}
#else
void binary_semaphore::wait() {
  std::unique_lock<std::mutex> lk(m_mtx);
  while (!m_signalled)
    m_cv.wait(lk);
  m_signalled = false;
}
void binary_semaphore::wake() {
  std::unique_lock<std::mutex> lk(m_mtx);
  m_signalled = true;
  m_cv.notify_one();
}
#endif

/* A thread helper structure, used in group commit lock below*/
struct group_commit_waiter_t {
  size_t m_value;
  binary_semaphore m_sema;
  group_commit_waiter_t *m_next;
  group_commit_waiter_t() : m_value(), m_sema(), m_next() {}
};

group_commit_lock::group_commit_lock()
    : m_mtx(), m_value(0), m_pending_value(0), m_lock(false), m_waiters_list() {
}

group_commit_lock::value_type group_commit_lock::value() const {
  return m_value.load(std::memory_order::memory_order_relaxed);
}

group_commit_lock::value_type group_commit_lock::pending() const {
  return m_pending_value.load(std::memory_order::memory_order_relaxed);
}

void group_commit_lock::set_pending(group_commit_lock::value_type num) {
  assert(num >= value());
  m_pending_value.store(num, std::memory_order::memory_order_relaxed);
}

const unsigned int MAX_SPINS = 1; /** max spins in acquire */
thread_local group_commit_waiter_t thread_local_waiter;

group_commit_lock::lock_return_code group_commit_lock::acquire(value_type num) {
  unsigned int spins = MAX_SPINS;

  for (;;) {
    if (num <= value()) {
      /* No need to wait.*/
      return lock_return_code::EXPIRED;
    }

    if (spins-- == 0)
      break;
    if (num > pending()) {
      /* Longer wait expected (longer than currently running operation),
        don't spin.*/
      break;
    }
    _mm_pause();
  }

  thread_local_waiter.m_value = num;
  std::unique_lock<std::mutex> lk(m_mtx, std::defer_lock);
  while (num > value()) {
    lk.lock();

    /* Re-read current value after acquiring the lock*/
    if (num <= value()) {
      return lock_return_code::EXPIRED;
    }

    if (!m_lock) {
      /* Take the lock, become group commit leader.*/
      m_lock = true;
      return lock_return_code::ACQUIRED;
    }

    /* Add yourself to waiters list.*/
    thread_local_waiter.m_next = m_waiters_list;
    m_waiters_list = &thread_local_waiter;
    lk.unlock();

    /* Sleep until woken in release().*/
    thread_local_waiter.m_sema.wait();
  }
  return lock_return_code::EXPIRED;
}

void group_commit_lock::release(value_type num) {
  std::unique_lock<std::mutex> lk(m_mtx);
  m_lock = false;

  /* Update current value. */
  assert(num >= value());
  m_value.store(num, std::memory_order_relaxed);

  /*
    Wake waiters for value <= current value.
    Wake one more waiter, who will become the group commit lead.
  */
  group_commit_waiter_t *cur, *prev, *next;
  group_commit_waiter_t *wakeup_list = nullptr;
  int extra_wake = 0;

  for (prev = nullptr, cur = m_waiters_list; cur; cur = next) {
    next = cur->m_next;
    if (cur->m_value <= num || extra_wake++ == 0) {
      /* Move current waiter to wakeup_list*/

      if (!prev) {
        /* Remove from the start of the list.*/
        m_waiters_list = next;
      } else {
        /* Remove from the middle of the list.*/
        prev->m_next = cur->m_next;
      }

      /* Append entry to the wakeup list.*/
      cur->m_next = wakeup_list;
      wakeup_list = cur;
    } else {
      prev = cur;
    }
  }
  lk.unlock();

  for (cur = wakeup_list; cur; cur = next) {
    next = cur->m_next;
    cur->m_sema.wake();
  }
}

} // namespace redo
