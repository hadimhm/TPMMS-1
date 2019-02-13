//  kwaymergesort.h
//  Created by Sina Pilehchiha on 2019-02-09.
//  Copyright © 2019 T9. All rights reserved.
#ifndef KWAYMERGESORT_H
#define KWAYMERGESORT_H
#include <iostream>
#include <fstream>
#include <sstream>
#include <queue>
#include <libgen.h> //  concerning the basename() function
struct CLAIM { //   a basic struct for a CLAIM entry.
    
    char clientID[10]; //   data
    char compensationAmount[10]; // data
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
        is.ignore(1); // ignore the whitespace character at the end of each line of input
        return is;
    }
};
struct CLAIM_SCHEMA { // the datatype class used by the priority_queue
    CLAIM datum; //  data
    std::istream *stream;
    bool (*comparisonFunction)(const CLAIM &a, const CLAIM &b);
    CLAIM_SCHEMA (const CLAIM &datum, //    constructor
                  std::istream *stream,
                  bool (*comparisonFunction)(const CLAIM &a, const CLAIM &b))
    :
    datum(datum),
    stream(stream),
    comparisonFunction(comparisonFunction) {}
    // comparison operator for maps keyed on this structure
    bool operator < (const CLAIM_SCHEMA &a) const
    {
         //recall that priority queues try to sort from
         //highest to lowest. thus, we need to negate.
         return !(datum < a.datum);
    }
};
struct KwayMergeSort {
KwayMergeSort(const std::string &inFile, // constructor, using CLAIM's overloaded < operator.  Must be defined.
              const std::string &outFile,
              int  maxBufferSize,
              std::string tempPath);
~KwayMergeSort(void); //    destructor
void Sort();            // Sort the data

std::string _inFile;
bool (*_compareFunction)(const CLAIM &a, const CLAIM &b);
std::string _tempPath;
std::vector<std::string> _vTempFileNames;
std::vector<std::ifstream*> _vTempFiles;
unsigned int _maxBufferSize;
unsigned int _runCounter;
std::string _outFile;
void DivideAndSort(); //    drives the creation of sorted sub-files stored on disk.
// drives the merging of the sorted temp files.
// final, sorted and merged output is written to an output file.
void Merge();
void WriteToTempFile(const std::vector<CLAIM> &lines);
void OpenTempFiles();
void CloseTempFiles();
};
KwayMergeSort::KwayMergeSort (const std::string &inFile, // constructor
                              const std::string &outFile,
                              int maxBufferSize,
                              std::string tempPath)
: _inFile(inFile)
, _outFile(outFile)
, _compareFunction(NULL)
, _tempPath(tempPath)
, _maxBufferSize(maxBufferSize)
, _runCounter(0) {}
KwayMergeSort::~KwayMergeSort(void) {} //   destructor
void KwayMergeSort::Sort() { // API for sorting.
    DivideAndSort();
    Merge();
}
std::string stl_basename(const std::string &path) { //   STLized version of basename() (because POSIX basename() modifies the input string pointer.)
    std::string result;
    char* path_dup = strdup(path.c_str());
    char* basename_part = basename(path_dup);
    result = basename_part;
    free(path_dup);
    size_t pos = result.find_last_of('.');
    if (pos != std::string::npos ) // checks whether pos is at the end of the sting or not.
        result = result.substr(0,pos); // updates the length of result.
    return result;
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
    for (size_t i=0; i < _vTempFiles.size(); ++i) { //  delete the pointers to the temp files.
        _vTempFiles[i]->close();
        delete _vTempFiles[i];
    }
    for (size_t i=0; i < _vTempFileNames.size(); ++i) { //  delete the temp files from the file system.
        remove(_vTempFileNames[i].c_str());  // remove = UNIX "rm"
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
void KwayMergeSort::DivideAndSort() {
    std::istream *input = new std::ifstream(_inFile.c_str(), std::ios::in);
    std::vector<CLAIM> lineBuffer;
    lineBuffer.reserve(_maxBufferSize);
    unsigned int totalBytes = 0;  // track the number of bytes consumed so far.
    CLAIM line;
    while (*input >> line) { // keep reading until there is no more input data
        lineBuffer.push_back(line); //  add the current line to the buffer and
        totalBytes += sizeof(line); //  track the memory used.
        if (totalBytes > _maxBufferSize - sizeof(line)) { //    sort the buffer and write to a temp file if we have filled up our quota
            sort(lineBuffer.begin(), lineBuffer.end()); // sort the buffer.
            WriteToTempFile(lineBuffer); // write the sorted data to a temp file
            lineBuffer.clear(); //  clear the buffer for the next run
            totalBytes = 0; // make the totalBytes counter zero in order to count the bytes occupying the buffer again.
        }
    }
    if (lineBuffer.empty() == false) {  //  handle the run (if any) from the last chunk of the input file.
        sort(lineBuffer.begin(), lineBuffer.end());
        WriteToTempFile(lineBuffer); // write the sorted data to a temp file
    }
}
void KwayMergeSort::Merge() { //    Merge the sorted temp files.
    // uses a priority queue, with the values being a pair of the record from the file, and the stream from which the record came
    // open the sorted temp files up for merging.
    // loads ifstream pointers into _vTempFiles
    std::ostream *output = new std::ofstream(_outFile.c_str(), std::ios::out);
    OpenTempFiles();
    std::priority_queue<CLAIM_SCHEMA> outQueue; //  priority queue for the buffer.
    CLAIM line; //  extract the first line from each temp file
    for (size_t i = 0; i < _vTempFiles.size(); ++i) {
        *_vTempFiles[i] >> line;
        outQueue.push(CLAIM_SCHEMA(line, _vTempFiles[i], _compareFunction));
    }
    // keep working until the queue is empty
    while (outQueue.empty() == false) {
        // grab the lowest element, print it, then ditch it.
        CLAIM_SCHEMA lowest = outQueue.top();
        // write the entry from the top of the queue
        *output << lowest.datum << std::endl;
        outQueue.pop(); //  remove this record from the queue
        *(lowest.stream) >> line; //    add the next line from the lowest stream (above) to the queue as long as it's not EOF.
        if (*(lowest.stream))
            outQueue.push( CLAIM_SCHEMA(line, lowest.stream, _compareFunction) );
    }
    CloseTempFiles();  //   clean up the temp files.
}
#endif /* KWAYMERGESORT_H */
