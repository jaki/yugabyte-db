//--------------------------------------------------------------------------------------------------
// Copyright (c) YugaByte, Inc.
//--------------------------------------------------------------------------------------------------

#include "yb/sql/util/sql_env.h"
#include "yb/sql/ptree/sem_context.h"

namespace yb {
namespace sql {

using std::shared_ptr;
using client::YBTable;
using client::YBTableName;
using client::YBColumnSchema;
using client::YBSchema;

//--------------------------------------------------------------------------------------------------

SemContext::SemContext(const char *sql_stmt,
                       size_t stmt_len,
                       ParseTree::UniPtr parse_tree,
                       SqlEnv *sql_env)
    : ProcessContext(sql_stmt, stmt_len, move(parse_tree)),
      symtab_(PTempMem()),
      sql_env_(sql_env),
      cache_used_(false),
      current_table_(nullptr),
      sem_state_(nullptr) {
}

SemContext::~SemContext() {
}

//--------------------------------------------------------------------------------------------------

CHECKED_STATUS SemContext::LookupTable(YBTableName name, shared_ptr<YBTable>* table,
                                       MCVector<ColumnDesc>* table_columns,
                                       int* num_key_columns, int* num_hash_key_columns,
                                       bool* is_system, bool write_only, const YBLocation& loc) {
  if (!name.has_namespace()) {
    if (CurrentKeyspace().empty()) {
      return Error(loc, ErrorCode::NO_NAMESPACE_USED);
    }

    name.set_namespace_name(CurrentKeyspace());
  }

  *is_system = name.is_system();
  if (*is_system && write_only && client::FLAGS_yb_system_namespace_readonly) {
    return Error(loc, ErrorCode::SYSTEM_NAMESPACE_READONLY);
  }

  VLOG(3) << "Loading table descriptor for " << name.ToString();
  *table = GetTableDesc(name);
  if (*table == nullptr) {
    return Error(loc, ErrorCode::TABLE_NOT_FOUND);
  }
  set_current_table(*table);

  const YBSchema& schema = (*table)->schema();
  const int num_columns = schema.num_columns();
  *num_key_columns = schema.num_key_columns();
  *num_hash_key_columns = schema.num_hash_key_columns();

  table_columns->resize(num_columns);
  for (int idx = 0; idx < num_columns; idx++) {
    // Find the column descriptor.
    const YBColumnSchema col = schema.Column(idx);
    (*table_columns)[idx].Init(idx,
                             schema.ColumnId(idx),
                             idx < *num_hash_key_columns,
                             idx < *num_key_columns,
                             col.is_static(),
                             col.is_counter(),
                             col.type(),
                             YBColumnSchema::ToInternalDataType(col.type()));

    // Insert the column descriptor to symbol table.
    MCString col_name(col.name().c_str(), col.name().size(), PTreeMem());
    RETURN_NOT_OK(MapSymbol(col_name, &(*table_columns)[idx]));
  }

  return Status::OK();

}

CHECKED_STATUS SemContext::MapSymbol(const MCString& name, PTColumnDefinition *entry) {
  if (symtab_[name].column_ != nullptr) {
    RETURN_NOT_OK(Error(entry->loc(), ErrorCode::DUPLICATE_COLUMN));
  }
  symtab_[name].column_ = entry;
  return Status::OK();
}

CHECKED_STATUS SemContext::MapSymbol(const MCString& name, PTAlterColumnDefinition *entry) {
  if (symtab_[name].alter_column_ != nullptr) {
    RETURN_NOT_OK(Error(entry->loc(), ErrorCode::DUPLICATE_COLUMN));
  }
  symtab_[name].alter_column_ = entry;
  return Status::OK();
}

CHECKED_STATUS SemContext::MapSymbol(const MCString& name, PTCreateTable *entry) {
  if (symtab_[name].create_table_ != nullptr) {
    RETURN_NOT_OK(Error(entry->loc(), ErrorCode::DUPLICATE_TABLE));
  }
  symtab_[name].create_table_ = entry;
  return Status::OK();
}

CHECKED_STATUS SemContext::MapSymbol(const MCString& name, ColumnDesc *entry) {
  if (symtab_[name].column_desc_ != nullptr) {
    LOG(FATAL) << "Entries of the same symbol are inserted"
               << ", Existing entry = " << symtab_[name].column_desc_
               << ", New entry = " << entry;
  }
  symtab_[name].column_desc_ = entry;
  return Status::OK();
}

shared_ptr<YBTable> SemContext::GetTableDesc(const client::YBTableName& table_name) {
  bool cache_used = false;
  shared_ptr<YBTable> table = sql_env_->GetTableDesc(table_name, &cache_used);
  if (table != nullptr) {
    parse_tree_->AddAnalyzedTable(table_name);
    if (cache_used) {
      // Remember cache was used.
      cache_used_ = true;
    }
  }
  return table;
}

SymbolEntry *SemContext::SeekSymbol(const MCString& name) {
  MCMap<MCString, SymbolEntry>::iterator iter = symtab_.find(name);
  if (iter != symtab_.end()) {
    return &iter->second;
  }
  return nullptr;
}

PTColumnDefinition *SemContext::GetColumnDefinition(const MCString& col_name) {
  const SymbolEntry * entry = SeekSymbol(col_name);
  if (entry == nullptr) {
    return nullptr;
  }
  return entry->column_;
}

const ColumnDesc *SemContext::GetColumnDesc(const MCString& col_name, bool reading_column) {
  SymbolEntry * entry = SeekSymbol(col_name);
  if (entry == nullptr) {
    return nullptr;
  }

  // To indicate that DocDB must read a columm value to execute an expression, the column is added
  // to the column_refs list.
  if (reading_column) {
    // TODO(neil) Currently AddColumnRef() relies on MCSet datatype to guarantee that we have a
    // unique list of IDs, but we should take advantage to "symbol table" when collecting data
    // for execution. Symbol table and "column_read_count_" need to be corrected so that we can
    // use MCList instead.

    // Indicate that this column must be read for the statement execution.
    current_dml_stmt_->AddColumnRef(*entry->column_desc_);
  }

  return entry->column_desc_;
}

//--------------------------------------------------------------------------------------------------

bool SemContext::IsConvertible(const PTExpr *expr, const std::shared_ptr<YQLType>& type) const {
  // TODO(Mihnea) Compatibility type check for collections might need further thoughts.
  switch (type->main()) {
    // Collection types : we only use conversion table for their elements
    case MAP: {
      // the empty set "{}" is a valid map expression
      if (expr->yql_type_id() == SET) {
        const PTSetExpr *set_expr = static_cast<const PTSetExpr *>(expr);
        return set_expr->elems().empty();
      }

      if (expr->yql_type_id() != MAP) {
        return expr->yql_type_id() == NULL_VALUE_TYPE;
      }
      shared_ptr<YQLType> keys_type = type->param_type(0);
      shared_ptr<YQLType> values_type = type->param_type(1);
      const PTMapExpr *map_expr = static_cast<const PTMapExpr *>(expr);
      for (auto &key : map_expr->keys()) {
        if (!IsConvertible(key, keys_type)) {
          return false;
        }
      }
      for (auto &value : map_expr->values()) {
        if (!IsConvertible(value, values_type)) {
          return false;
        }
      }

      return true;
    }

    case SET: {
      if (expr->yql_type_id() != SET) {
        return expr->yql_type_id() == NULL_VALUE_TYPE;
      }
      shared_ptr<YQLType> elem_type = type->params()[0];
      const PTSetExpr *set_expr = static_cast<const PTSetExpr*>(expr);
      for (auto &elem : set_expr->elems()) {
        if (!IsConvertible(elem, elem_type)) {
          return false;
        }
      }
      return true;
    }

    case LIST: {
      if (expr->yql_type_id() != LIST) {
        return expr->yql_type_id() == NULL_VALUE_TYPE;
      }
      shared_ptr<YQLType> elem_type = type->params()[0];
      const PTListExpr *list_expr = static_cast<const PTListExpr*>(expr);
      for (auto &elem : list_expr->elems()) {
        if (!IsConvertible(elem, elem_type)) {
          return false;
        }
      }
      return true;
    }

    case TUPLE:
      LOG(FATAL) << "Tuple type not support yet";
      return false;

    // Elementary types : we directly check conversion table
    default:
      return YQLType::IsImplicitlyConvertible(type->main(), expr->yql_type_id());
  }
}

bool SemContext::IsComparable(DataType lhs_type, DataType rhs_type) const {
  return YQLType::IsComparable(lhs_type, rhs_type);
}

}  // namespace sql
}  // namespace yb
