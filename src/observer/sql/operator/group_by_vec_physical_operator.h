/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "sql/expr/aggregate_hash_table.h"
#include "sql/operator/physical_operator.h"

/**
 * @brief Group By 物理算子(vectorized)
 * @ingroup PhysicalOperator
 */
class GroupByVecPhysicalOperator : public PhysicalOperator
{
public:
  GroupByVecPhysicalOperator(
      std::vector<std::unique_ptr<Expression>> &&group_by_exprs, std::vector<Expression *> &&expressions)
      : group_by_exprs_(std::move(group_by_exprs)), aggregate_expressions_(expressions), hash_table_(expressions){};

  virtual ~GroupByVecPhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::GROUP_BY_VEC; }

  RC open(Trx *trx) override
  {
    ASSERT(children_.size() == 1, "group by operator only support one child, but got %d", children_.size());
    PhysicalOperator &child = *children_[0];
    RC                rc    = child.open(trx);
    if (OB_FAIL(rc)) {
      LOG_INFO("failed to open child operator. rc=%s", strrc(rc));
      return rc;
    }
    while (OB_SUCC(rc = child.next(chunk_))) {
      Chunk group_chunk, aggrs_chunk;
      for (int i = 0; i < group_by_exprs_.size(); i++) {
        auto group_col = std::make_unique<Column>();
        group_by_exprs_[i]->get_column(chunk_, *group_col);
        group_chunk.add_column(std::move(group_col), i);
      }
      for (int i = 0; i < aggregate_expressions_.size(); i++) {
        auto aggr_col = std::make_unique<Column>();
        aggregate_expressions_[i]->get_column(chunk_, *aggr_col);
        aggrs_chunk.add_column(std::move(aggr_col), i);
      }

      rc = hash_table_.add_chunk(group_chunk, aggrs_chunk);
      if(OB_FAIL(rc)){
        return rc;
      }
    }
    scanner_ = new StandardAggregateHashTable::Scanner(&hash_table_);
    scanner_->open_scan();
    return RC::SUCCESS;
  }
  RC next(Chunk &chunk) override
  {
    RC rc = scanner_->next(chunk);
    if (OB_FAIL(rc)) {
      return rc;
    }
    return RC::SUCCESS;
  }

  RC close() override
  {
    children_[0]->close();
    free(scanner_);
    return RC::SUCCESS;
  }

private:
  std::vector<std::unique_ptr<Expression>> group_by_exprs_;
  std::vector<Expression *>                aggregate_expressions_;
  Chunk                                    chunk_;
  StandardAggregateHashTable::Scanner     *scanner_;
  StandardAggregateHashTable               hash_table_;
};