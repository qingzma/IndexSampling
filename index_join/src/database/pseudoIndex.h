//
// Created by Scott on 2019-08-20.
// Alright reserved by Qinghzi Ma
// Email: Q.Ma.2@warwick.ac.uk
// Department of Computer Science, University of Warwick,  UK
//

#ifndef MAIN_MEM_JOIN_PSEUDOINDEX_H
#define MAIN_MEM_JOIN_PSEUDOINDEX_H

#include <vector>
#include <list>
#include <map>
#include <string>
#include <random>

#include "Table.h"




class PseudoIndexBuilder {
public:
    PseudoIndexBuilder();
    PseudoIndexBuilder(int sampleSize);
    ~PseudoIndexBuilder(){};
    int AppendTable(std::shared_ptr<Table> table, int RHSIndex, int LHSIndex, int tableNumber);
    void Build();
    void Sample(int sampleSize);
    int64_t getCardinality();

private:
    int sampleSize;
    std::vector<std::shared_ptr<Table> > m_joinedTables;
    std::vector<int> m_LHSJoinIndex;
    std::vector<int> m_RHSJoinIndex;
    std::map<std::string, int64_t> m_join_counts;
    int64_t m_cadinality;


};


#endif //MAIN_MEM_JOIN_PSEUDOINDEX_H
