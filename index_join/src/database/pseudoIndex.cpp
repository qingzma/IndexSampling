//
// Created by Scott on 2019-08-20.
// Alright reserved by Qinghzi Ma
// Email: Q.Ma.2@warwick.ac.uk
// Department of Computer Science, University of Warwick,  UK
//

#include "pseudoIndex.h"
#include "../util/StringSplitter.h"

#include <iostream>


PseudoIndexBuilder::PseudoIndexBuilder() {
    this->sampleSize = 100;
    m_join_counts={};
}

PseudoIndexBuilder::PseudoIndexBuilder(int sampleSize) {
    this->sampleSize=sampleSize;
    m_join_counts={};
}

int PseudoIndexBuilder::AppendTable(std::shared_ptr<Table> table, int RHSIndex, int LHSIndex, int tableNumber) {
    m_joinedTables.push_back(table);
    m_LHSJoinIndex.push_back(LHSIndex);
    m_RHSJoinIndex.push_back(RHSIndex);

    return int(m_joinedTables.size());
}

void PseudoIndexBuilder::Build(){
    int n_tables = m_LHSJoinIndex.size();

    if (m_RHSJoinIndex.at(0) != -1 || m_LHSJoinIndex.at(n_tables-1) != -1){
        std::cout<<m_LHSJoinIndex.at(0)<<m_RHSJoinIndex.at(n_tables-1)<<std::endl;
        throw std::runtime_error("The join tables are not in the correct order!");
    }

    int64_t count = 0;

    auto table0= m_joinedTables.begin();
    std::shared_ptr < key_index > table0Index = table0->get()->get_key_index(m_LHSJoinIndex.at(0));

    // initialize m_join_counts
    for (key_index::iterator it = table0Index->begin(); it!= table0Index->end(); it++){
        try {
            m_join_counts.at("$_"+std::to_string(it->first)) += 1;
        } catch (std::out_of_range e){
            m_join_counts.emplace("$_"+std::to_string(it->first), 1);
        }
    }

    for (auto item: m_join_counts){
        std::cout<<item.first<<", "<<item.second<<std::endl;
    }


    for (int table_count=1; table_count<2;table_count++){//n_tables-1
            std::shared_ptr< key_index > tableIIndex = m_joinedTables.at(table_count)->get_key_index(m_RHSJoinIndex.at(table_count));
            std::shared_ptr< complex_key_index > tableIComplexIndex = m_joinedTables.at(table_count)->get_complex_key_index(m_RHSJoinIndex.at(table_count),m_LHSJoinIndex.at(table_count));
            std::shared_ptr< join_attributes_relation_index> tableIJoinIndex = m_joinedTables.at(table_count)->get_join_attribute_relation_index(m_RHSJoinIndex.at(table_count),m_LHSJoinIndex.at(table_count));
        // iterate over m_join_counts
//        for (auto item:m_join_counts){
//            int64_t LTableLKey = StringSplitter::split_last_int64(item.first, "_");
//
//            // if there is no join pair in the right table, delete the records in m_join_counts
//
//            std::set<int64_t > RTableRKeys = tableIJoinIndex->at(LTableLKey);
//            if (RTableRKeys.empty()){
//                std::cout<<"erase "<<item.first<<std::endl;
//                m_join_counts.erase(item.first);
//            } else {
//                for (int64_t RTableRkey: RTableRKeys){
//                    std::tuple<jfkey_t,jfkey_t> complex_key {LTableLKey, RTableRkey};
//                    m_join_counts[item.first+"-$_"+std::to_string(RTableRkey)]= m_join_counts[item.first] * tableIComplexIndex->count(complex_key);
//                }
//                m_join_counts.erase(item.first);
//            }
//        }

        for (auto item: m_join_counts){
            std::cout<<item.first<<","<<item.second<<std::endl;
        }

    }



//    for (key_index::iterator it = table0Index->begin(); it!= table0Index->end(); it++){
//        // std::cout<<it->first<<","<<it->second<<std::endl;
//        // loop over each bi-directional joined table.
//        for (int table_count=1; table_count<n_tables-1;table_count++){
//            std::shared_ptr< key_index > tableIIndex = m_joinedTables.at(table_count)->get_key_index(m_RHSJoinIndex.at(table_count));
//            std::shared_ptr< complex_key_index > tableIComplexIndex = m_joinedTables.at(table_count)->get_complex_key_index(m_RHSJoinIndex.at(table_count),m_LHSJoinIndex.at(table_count));
//            std::shared_ptr< join_attributes_relation_index> tableIJoinIndex = m_joinedTables.at(table_count)->get_join_attribute_relation_index(m_RHSJoinIndex.at(table_count),m_LHSJoinIndex.at(table_count));
//        }
//    }







//    int count = 0;
//    for (std::vector<std::shared_ptr<Table>>::iterator it = m_joinedTables.begin();it != m_joinedTables.end(); ++it){
//        it->get().
//     }

};
std::string PseudoIndexBuilder::Sample() { return "";};

