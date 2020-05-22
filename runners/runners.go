package runners

// TempDirHandler is a handler that receives the path of a temporary
// directory as parameter
type TempDirHandler func(string)

// TempSubDirsHandler is a handler that receives an array of directory paths
// as parameter
type TempSubDirsHandler func([]string)
