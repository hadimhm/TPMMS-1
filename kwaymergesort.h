//  kwaymergesort.h
//  Created by Sina Pilehchiha on 2019-02-09.
//  Copyright © 2019 T9. All rights reserved.
#ifndef KWAYMERGESORT_H
#define KWAYMERGESORT_H
#include <iostream>
#include <fstream>
#include <algorithm>
#include <sstream>
#include <vector>
#include <queue>
#include <sys/stat.h>
#include <libgen.h> //  concerning the basename() function
struct CLAIM { //   a basic struct for a CLAIM entry.
    char clientID[10];
    char compensationAmount[10];
    bool operator < (const CLAIM &b) const //   overload the < (less than) operator for comparison between CLAIM records
    {
        if      (atoi(clientID) < atoi(b.clientID))  return true; //    convert clientID character arrays into integers in order to be able to compare their values.
        else if (atoi(clientID) > atoi(b.clientID))  return false;
        //  the program gets here when clientIDs are the same. now it tries to sort based on compensationAmounts.
        if      (atof(compensationAmount) < atof(b.compensationAmount))  return true; //    convert compensationAmount character arrays into floats in order to be able to compare their values.
        else return false;
    }
    friend std::ostream& operator<<(std::ostream &os, const CLAIM &b) //    overload the << operator for writing a CLAIM struct
    {
        os << std::string(18, ' ') << b.clientID << std::string(214, ' ') << b.compensationAmount;
        return os;
    }
    friend std::istream& operator>>(std::istream &is, CLAIM &b) //  overload the >> operator for reading into a CLAIM struct
    {
        is.ignore(18);
        is.get(b.clientID, 10);
        is.ignore(214);
        is.get(b.compensationAmount, 10);
        is.ignore(1); // ignores the whitespace character at the end of each line of input
        return is;
    }
};
std::string stl_basename(const std::string& path); //   STLized version of basename() (because POSIX basename() modifies the input string pointer.)
class MERGE_DATA {
public:
    // data
    CLAIM data;
    std::istream *stream;
    bool (*compFunc)(const CLAIM &a, const CLAIM &b);
    // constructor
    MERGE_DATA (const CLAIM &data,
                std::istream *stream,
                bool (*compFunc)(const CLAIM &a, const CLAIM &b))
    :
    data(data),
    stream(stream),
    compFunc(compFunc)
    {}
    // comparison operator for maps keyed on this structure
    bool operator < (const MERGE_DATA &a) const
    {
        // recall that priority queues try to sort from
        // highest to lowest. thus, we need to negate.
        return !(data < a.data);
    }
};
class KwayMergeSort {
public:
    // constructor, using CLAIM's overloaded < operator.  Must be defined.
    KwayMergeSort(const std::string &inFile,
                  const std::string &outFile,
                  int  maxBufferSize  = 2000,
                  bool compressOutput = false,
                  std::string tempPath     = "./");
    // destructor
    ~KwayMergeSort(void);
    void Sort();            // Sort the data
    void SetBufferSize(int bufferSize);   // change the buffer size
    void SetComparison(bool (*compareFunction)(const CLAIM &a, const CLAIM &b));   // change the sort criteria
private:
    std::string _inFile;
    bool (*_compareFunction)(const CLAIM &a, const CLAIM &b);
    std::string _tempPath;
    std::vector<std::string>    _vTempFileNames;
    std::vector<std::ifstream*>  _vTempFiles;
    unsigned int _maxBufferSize;
    unsigned int _runCounter;
    bool _compressOutput;
    bool _tempFileUsed;
    std::string _outFile;
    // drives the creation of sorted sub-files stored on disk.
    void DivideAndSort();
    // drives the merging of the sorted temp files.
    // final, sorted and merged output is written to "out".
    void Merge();
    void WriteToTempFile(const std::vector<CLAIM> &lines);
    void OpenTempFiles();
    void CloseTempFiles();
};
// constructor
KwayMergeSort::KwayMergeSort (const std::string &inFile,
                              const std::string &outFile,
                              int maxBufferSize,
                              bool compressOutput,
                              std::string tempPath)
: _inFile(inFile)
, _outFile(outFile)
, _compareFunction(NULL)
, _tempPath(tempPath)
, _maxBufferSize(maxBufferSize)
, _runCounter(0)
, _compressOutput(compressOutput)
{}
// destructor
KwayMergeSort::~KwayMergeSort(void)
{}
// API for sorting.
void KwayMergeSort::Sort() {
    DivideAndSort();
    Merge();
}
// change the buffer size used for sorting
void KwayMergeSort::SetBufferSize (int bufferSize) {
    _maxBufferSize = bufferSize;
}
// change the sorting criteria
void KwayMergeSort::SetComparison (bool (*compareFunction)(const CLAIM &a, const CLAIM &b)) {
    _compareFunction = compareFunction;
}
void KwayMergeSort::DivideAndSort() {
    std::istream *input = new std::ifstream(_inFile.c_str(), std::ios::in);
    if ( input->good() == false ) { //  // bail unless the file is legit
        std::cerr << "Error: The requested input file (" << _inFile << ") could not be opened. Exiting!" << std::endl;
        exit (1);
    }
    std::vector<CLAIM> lineBuffer;
    lineBuffer.reserve(_maxBufferSize);
    unsigned int totalBytes = 0;  // track the number of bytes consumed so far.
    // track whether or not we actually had to use a temp
    // file based on the memory that was allocated
    _tempFileUsed = false;
    CLAIM line;
    while (*input >> line) { // keep reading until there is no more input data
        // add the current line to the buffer and track the memory used.
        lineBuffer.push_back(line);
        totalBytes += sizeof(line);  // buggy?
        // sort the buffer and write to a temp file if we have filled up our quota
        if (totalBytes > _maxBufferSize - sizeof(line)) {
            if (_compareFunction != NULL)
                sort(lineBuffer.begin(), lineBuffer.end(), *_compareFunction);
            else
                sort(lineBuffer.begin(), lineBuffer.end());
            // write the sorted data to a temp file
            WriteToTempFile(lineBuffer);
            // clear the buffer for the next run
            lineBuffer.clear();
            _tempFileUsed = true;
            totalBytes = 0;
        }
    }
    // handle the run (if any) from the last chunk of the input file.
    if (lineBuffer.empty() == false) {
        // write the last "chunk" to the tempfile if
        // a temp file had to be used (i.e., we exceeded the memory)
        if (_tempFileUsed == true) {
            if (_compareFunction != NULL)
                sort(lineBuffer.begin(), lineBuffer.end(), *_compareFunction);
            else
                sort(lineBuffer.begin(), lineBuffer.end());
            // write the sorted data to a temp file
            WriteToTempFile(lineBuffer);
            //WriteToTempFile(lineBuffer);
        }
    }
}
void KwayMergeSort::WriteToTempFile(const std::vector<CLAIM> &lineBuffer) {
    // name the current tempfile
    std::stringstream tempFileSS;
    if (_tempPath.size() == 0)
        tempFileSS << _inFile << "." << _runCounter;
    else
        tempFileSS << _tempPath << "/" << stl_basename(_inFile) << "." << _runCounter;
    std::string tempFileName = tempFileSS.str();
    // do we want a regular or a gzipped tempfile?
    std::ofstream *output;
    //if (_compressOutput == true)
    //output = new ogzstream(tempFileName.c_str(), ios::out);
    //else
    output = new std::ofstream(tempFileName.c_str(), std::ios::out);
    // write the contents of the current buffer to the temp file
    for (size_t i = 0; i < lineBuffer.size(); ++i) {
        *output << lineBuffer[i] << std::endl;
    }
    // update the tempFile number and add the tempFile to the list of tempFiles
    ++_runCounter;
    output->close();
    delete output;
    _vTempFileNames.push_back(tempFileName);
}
void KwayMergeSort::Merge() { //    Merge the sorted temp files.
    // uses a priority queue, with the values being a pair of the record from the file, and the stream from which the record came
    // open the sorted temp files up for merging.
    // loads ifstream pointers into _vTempFiles
    std::ostream *output = new std::ofstream(_outFile.c_str(), std::ios::out);
    OpenTempFiles();
    // priority queue for the buffer.
    std::priority_queue< MERGE_DATA > outQueue;
    // extract the first line from each temp file
    CLAIM line;
    for (size_t i = 0; i < _vTempFiles.size(); ++i) {
        *_vTempFiles[i] >> line;
        outQueue.push( MERGE_DATA(line, _vTempFiles[i], _compareFunction) );
    }
    // keep working until the queue is empty
    while (outQueue.empty() == false) {
        // grab the lowest element, print it, then ditch it.
        MERGE_DATA lowest = outQueue.top();
        // write the entry from the top of the queue
        *output << lowest.data << std::endl;
        // remove this record from the queue
        outQueue.pop();
        // add the next line from the lowest stream (above) to the queue
        // as long as it's not EOF.
        *(lowest.stream) >> line;
        if (*(lowest.stream))
            outQueue.push( MERGE_DATA(line, lowest.stream, _compareFunction) );
    }
    // clean up the temp files.
    CloseTempFiles();
}
void KwayMergeSort::OpenTempFiles() {
    for (size_t i=0; i < _vTempFileNames.size(); ++i) {
        std::ifstream *file = nullptr;
        file = new std::ifstream(_vTempFileNames[i].c_str(), std::ios::in);
        if (file->good() == true) {
            // add a pointer to the opened temp file to the list
            _vTempFiles.push_back(file);
        }
        else {
            std::cerr << "Unable to open temp file (" << _vTempFileNames[i]
            << ").  I suspect a limit on number of open file handles.  Exiting."
            << std::endl;
            exit(1);
        }
    }
}
void KwayMergeSort::CloseTempFiles() {
    // delete the pointers to the temp files.
    for (size_t i=0; i < _vTempFiles.size(); ++i) {
        _vTempFiles[i]->close();
        delete _vTempFiles[i];
    }
    // delete the temp files from the file system.
    for (size_t i=0; i < _vTempFileNames.size(); ++i) {
        remove(_vTempFileNames[i].c_str());  // remove = UNIX "rm"
    }
}
std::string stl_basename(const std::string &path) {
    std::string result;
    char* path_dup = strdup(path.c_str());
    char* basename_part = basename(path_dup);
    result = basename_part;
    free(path_dup);
    size_t pos = result.find_last_of('.');
    if (pos != std::string::npos ) // checks whether pos is yet at the end of the sting or not.
        result = result.substr(0,pos); // updates the length of result.
    return result;
}
#endif /* KWAYMERGESORT_H */
