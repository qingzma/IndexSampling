//
// Created by Scott on 2019-08-21.
// Alright reserved by Qinghzi Ma
// Email: Q.Ma.2@warwick.ac.uk
// Department of Computer Science, University of Warwick,  UK
//

#ifndef MAIN_MEM_JOIN_STRINGSPLITTER_H
#define MAIN_MEM_JOIN_STRINGSPLITTER_H

#include <string>
#include <vector>
#include <string.h>
#include <stdio.h>

class StringSplitter{
public:
    static std::vector<std::string> split(std::string str,std::string sep){
        char* cstr=const_cast<char*>(str.c_str());
        char* current;
        std::vector<std::string> arr;
        current=strtok(cstr,sep.c_str());
        while(current!=NULL){
            arr.push_back(current);
            current=strtok(NULL,sep.c_str());
        }
        return arr;
    }

    static int64_t split_last_int64(std::string str,std::string sep){

        std::vector<std::string> arr = split(str, sep);
        auto last = arr[arr.size()-1].c_str();
//        printf("last string split is %s\n",last);
        return std::stoi(last);
    }
};


#endif //MAIN_MEM_JOIN_STRINGSPLITTER_H
