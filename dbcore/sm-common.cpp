#include "sm-common.h"
#include <string.h>
#include "rcu.h"

#include <glog/logging.h>

#include <cstdio>
#include <cstdlib>
#include <cerrno>
#include <unistd.h>
#include <sys/fcntl.h>
#include <vector>
#include <cstring>
#include <iostream>
#include <libpmem.h>
#include <map>
#include "sm-config.h"
#include "sm-log-alloc.h"


#define LAYOUT_NAME "intro_1"
//#define PMEM_LEN 17179869184
#define PMEM_LEN (config::log_segment_mb *1024 *1024)
#define PMEM_FILE 1024
char other_file[64]="/mnt/pmem0/ermiadb_log/";
int seq=0;
std::map<int, char*> pmem;
std::map<std::string, int> pmem_reverse;
int segment_number =0;
int switch_number = 10;
extern int need_switch1;
extern int need_switch2;
extern int need_switch3;
extern int need_switch4;
extern int need_switch5;
extern int need_switch6;
extern int need_switch7;
extern int need_switch8;
int node_has_change =0;

namespace ermia {

void die(char const *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  va_end(ap);
  std::abort();
}

char *os_asprintf(char const *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  DEFER(va_end(ap));

  char *msg;
  int err = vasprintf(&msg, fmt, ap);
  THROW_IF(err < 0, os_error, errno, "Unable to format string template: %s",
           fmt);
  return msg;
}

int os_open(char const *path, int flags) {
  int fd = open(path, flags);
  LOG_IF(FATAL, fd < 0) << "Unable to open file " << path << "(" << fd << ")";
  return fd;
}

int os_openat(int dfd, char const *fname, int flags) {
  
  char file_name[64], fname1[64];
  std::string fname2= fname;
  strcpy(fname1, fname);
  //int len = strlen(fname1);
  //std::cerr<<"len:   "<<len<<std::endl;
  //std::cerr<<"open      fname1:  "<<fname1<<"    pmem_reverse.count(fname1):  "<< pmem_reverse.count(fname1)<<std::endl;
  //int fd = openat(dfd, fname, flags, S_IRUSR | S_IWUSR);
  //LOG_IF(FATAL, fd < 0) << "Unable to open file " << fname << "(" << fd << ")";
  
  if(flags & O_CREAT && !pmem_reverse.count(fname2))
  {
    segment_number++;
    if(segment_number > switch_number)
    {
      strcpy(file_name, other_file);
      if(node_has_change == 0)
      {
        need_switch1 = 1;
        need_switch2 = 1;
        need_switch3 = 1;
        need_switch4 = 1;
        need_switch5 = 1;
        need_switch6 = 1;
        need_switch7 = 1;
        need_switch8 = 1;  
        node_has_change = 1;    
      }   
    }
    else
    {
      char *file2 = (char*)(ermia::config::log_dir.data());
      strcpy(file_name, file2);
    }

    strcat(file_name, fname);
    //std::cerr<< file_name<< std::endl;
    char *pmemaddr;
    size_t mapped_len;
    int is_pmem;

    if ((pmemaddr = (char*)pmem_map_file(file_name, PMEM_LEN, PMEM_FILE_CREATE,
                0666, &mapped_len, &is_pmem)) == NULL) {
        std::cerr<< "pmem_map_file error"<< std::endl;
      }
    pmem[seq] = pmemaddr;
     
    pmem_reverse[fname2] = seq;

    int fd=seq;
    ++seq;
    //std::cerr<<"create      seq: "<< seq<<"    fname:  "<< fname1<<"   openfd:   "<< fd<<std::endl;
    return fd;
  }
  int fd= pmem_reverse[fname2];
  //std::cerr<<"openfd:   "<<fd<<std::endl;
  return fd;

  
}

void os_write(int fd, void const *buf, size_t bufsz) {
  //size_t err = write(fd, buf, bufsz);
  //LOG_IF(FATAL, err != bufsz) << "Error writing " << bufsz << " bytes to file";
  
  char *pmemaddr = pmem[fd];
  pmem_memcpy_persist(pmemaddr, buf, bufsz);
  size_t n = bufsz;
}

size_t os_pwrite(int fd, char const *buf, size_t bufsz, off_t offset) {
  //std::cerr<<"write size:   "<<bufsz<<std::endl;
  char *pmemaddr = pmem[fd];
  pmemaddr += offset;
  pmem_memcpy_persist(pmemaddr, buf, bufsz);
  size_t n = bufsz;
  
  /*
  size_t n = 0;
  while (n < bufsz) {
    ssize_t m = pwrite(fd, buf + n, bufsz - n, offset + n);
    if (not m) break;
    LOG_IF(FATAL, m < 0) << "Error writing " << bufsz << " bytes at offset "
                         << offset << "(" << errno << ")";
    THROW_IF(m < 0, os_error, errno,
             "Error writing %zd bytes to file at offset %zd", bufsz, offset);
    n += m;
  }*/
  
  return n;
}

size_t os_pread(int fd, char *buf, size_t bufsz, off_t offset) {
  size_t n = 0;
  /*
  while (n < bufsz) {
    ssize_t m = pread(fd, buf + n, bufsz - n, offset + n);
    if (not m) break;
    LOG_IF(FATAL, m < 0)
      << "Error reading " << bufsz << " bytes from file at offset " << offset;
    n += m;
  }*/
  char *pmemaddr = pmem[fd];
  pmemaddr += offset;
  memcpy(buf, pmemaddr, bufsz);
  //pmem_memcpy(buf, pmemaddr, bufsz, PMEM_F_MEM_NODRAIN);
  n= bufsz;
  return n;
}

void os_truncate(char const *path, size_t size) {
  int err = truncate(path, size);
  THROW_IF(err, os_error, errno, "Error truncating file %s to %zd bytes", path,
           size);
}

void os_truncateat(int dfd, char const *path, size_t size) {
  if(path[0] == 'l')
    return;
  int fd = os_openat(dfd, path, O_WRONLY | O_CREAT);
  DEFER(os_close(fd));

  char file_name[64];
  //strcpy(file_name, file);
  //char *file2 = (char*)(ermia::config::log_dir.data());
  //strcpy(file_name, file2);
  if(segment_number > switch_number)
    {
      strcpy(file_name, other_file);      
    }
    else
    {
      char *file2 = (char*)(ermia::config::log_dir.data());
      strcpy(file_name, file2);
    }

  strcat(file_name, path);
  int err = truncate(file_name, size);
  /*
  if(size == 0)
  {
    char path1[64];
    strcpy(path1, path);
    int fd1 = pmem_reverse[path1];
    pmem.erase(fd1);
  }*/
  //std::cerr<<"dfd : "<<dfd<<"   fd : "<<fd<<"   path : "<< path<<"   size : "<<size<<std::endl;
  /*std::map<char*, int>::reverse_iterator   iter;
     for(iter = pmem_reverse.rbegin(); iter != pmem_reverse.rend(); iter++){
          std::cerr<<iter->first<<"  ->  "<<iter->second<<std::endl;
     }
     */
  THROW_IF(err, os_error, errno, "Error truncating file %s to %zd bytes", path,
           size);
  
  //int err = ftruncate(fd, size);
  //THROW_IF(err, os_error, errno, "Error truncating file %s to %zd bytes", path,
  //         size);
}

void os_renameat(int fromfd, char const *from, int tofd, char const *to) {
  char fromfile[64], tofile[64];
  //strcpy(fromfile, file);
  //char *file2 = (char*)(ermia::config::log_dir.data());
  //strcpy(fromfile, file2);
  //strcpy(tofile, file2);
  if(segment_number > switch_number && from[0] == 'n')
    {
      strcpy(fromfile, other_file);
      strcpy(tofile, other_file);
      
    }
    else
    {
      char *file2 = (char*)(ermia::config::log_dir.data());
      strcpy(fromfile, file2);
      strcpy(tofile, file2);
    }

  strcat(fromfile, from);
  //strcpy(tofile, file);
  strcat(tofile, to);
  int err = rename(fromfile, tofile);

  char from1[64], to1[64];
  strcpy(from1, from);
  strcpy(to1, to);
  std::string from2, to2;
  from2 = from;
  to2 = to;
  int fd = pmem_reverse[from2];
  pmem_reverse[to2] = fd;
  
  //int err = renameat(fromfd, from, tofd, to);
  //std::cerr<<"from : "<<from<<"      to : "<<to1<<"     fd: "<<pmem_reverse[to1] <<"   pmem_reverse.count(to1):  "<< pmem_reverse.count(to1) <<std::endl;
  //std::cerr<<"fromfd : "<<fromfd<<"      tofd : "<<tofd<<std::endl;
  THROW_IF(err, os_error, errno, "Error renaming file %s to %s", from, to);
}

void os_unlinkat(int dfd, char const *fname, int flags) {
  char file_name[64];
  //strcpy(file_name, file);
  //char *file2 = (char*)(ermia::config::log_dir.data());
  //strcpy(file_name, file2);
  if(segment_number > switch_number)
    {
      strcpy(file_name, other_file);      
    }
    else
    {
      char *file2 = (char*)(ermia::config::log_dir.data());
      strcpy(file_name, file2);
    }

  strcat(file_name, fname);
  int err = unlink(file_name);
  //int err = unlinkat(dfd, fname, flags);
  THROW_IF(err, os_error, errno, "Error unlinking file %s", fname);
}

#warning os_fsync is not guaranteed to actually do anything (thanks, POSIX)
/* ^^^

   Turns out that POSIX makes no guarantees about what fsync does. In
   the worst case, a no-op implementation is explicitly allowed by the
   standard, and Mac OS X is a real life example.

   The eventual solution will be to add a whole bunch of extra code
   here, covering all possible implementation deficiencies of
   platforms we support. For now, we just ignore the problem.
 */
void os_fsync(int fd) {
  //int err = fsync(fd);
  //THROW_IF(err, os_error, errno, "Error synching fd %d to disk", fd);
}

void os_close(int fd) {
  //int err = close(fd);
  //THROW_IF(err, os_error, errno, "Error closing fd %d", fd);

  //char *pmemaddr = pmem[fd];
  //pmem_unmap(pmemaddr, PMEM_LEN);
  //pmem.erase(fd);
  //pmem_reverse.erase(pmemaddr);
}

void os_finish() {
     std::map<int, char*>::reverse_iterator   iter;
     for(iter = pmem.rbegin(); iter != pmem.rend(); iter++){
          std::cerr<<iter->first<<" "<<iter->second<<std::endl;
          pmem_unmap(iter->second, PMEM_LEN);
     }

     pmem.clear();
     pmem_reverse.clear();

}

int os_dup(int fd) {
  int rval = dup(fd);
  THROW_IF(rval < 0, os_error, errno, "Unable to duplicate fd %d", fd);
  return rval;
}

size_t os_snprintf(char *dest, size_t size, char const *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  DEFER(va_end(ap));

  int n = vsnprintf(dest, size, fmt, ap);
  THROW_IF(n < 0, os_error, errno, "Unable to format string template: %s", fmt);
  if (size) dest[size - 1] = 0;  // in case user doesn't test return value!
  return n;
}

dirent_iterator::dirent_iterator(char const *dname)
    : _d(opendir(dname)), used(false) {
  THROW_IF(not _d, os_error, errno, "Unable to open/create directory: %s",
           dname);
}

dirent_iterator::~dirent_iterator() {
  int err = closedir(_d);
  WARN_IF(err, "Closing dirent iterator gave errno %d", errno);
}

void dirent_iterator::iterator::operator++() {
  errno = 0;
  _dent = readdir(_d);
  if (not _dent) {
    THROW_IF(errno, os_error, errno, "Error during directory scan");
    _d = NULL;
  }
}

dirent_iterator::iterator dirent_iterator::begin() {
  if (used) rewinddir(_d);

  used = true;
  iterator rval{_d, NULL};
  ++rval;  // prime it
  return rval;
}

dirent_iterator::iterator dirent_iterator::end() {
  return iterator{NULL, NULL};
}

int dirent_iterator::dup() { return os_dup(dirfd(_d)); }

tmp_dir::tmp_dir() {
  strcpy(dname, "/tmp/test-log-XXXXXX");
  THROW_IF(not mkdtemp(dname), os_error, errno,
           "Unable to create temporary directory %s", dname);
}

tmp_dir::~tmp_dir() {
  std::vector<std::string> fname_list;
  dirent_iterator it(dname);
  int dfd = it.dup();
  DEFER(close(dfd));
  for (auto *fname : it) {
    if (fname[0] != '.') fname_list.push_back(fname);
  }

  for (auto &fname : fname_list) {
    fprintf(stderr, "Deleting %s/%s\n", dname, fname.c_str());
    unlinkat(dfd, fname.c_str(), 0);
  }

  fprintf(stderr, "Deleting directory %s/\n", dname);
  rmdir(dname);
}
}  // namespace ermia
