package runners

import (
	"io/ioutil"

	"github.com/google/uuid"
	"github.com/rhizomplatform/fs"
)

// WithTempDir runs the specified handler in a context with a
// temporary directory available
func WithTempDir(handler TempDirHandler) {
	if dir, err := ioutil.TempDir("", ""); err != nil {
		panic(err)
	} else {
		defer fs.RemoveAll(dir)
		handler(dir)
	}
}

// WithTempSubDirs runs the specified handler in a context with a
// number of temporary subdirectories available
func WithTempSubDirs(count int, handler TempSubDirsHandler) {
	WithTempDir(func(path string) {
		dirs := make([]string, count)

		for i := 0; i < count; i++ {
			d := fs.Path(path).Join(uuid.New().String())
			if err := d.MkdirAll(); err != nil { //FIXED: Using the new 'fs' package
				panic(err)
			}
			dirs[i] = d.String()
		}

		handler(dirs)
	})
}
