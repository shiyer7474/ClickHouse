#include <memory>
#include <Databases/IDatabase.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/TableNameHints.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/IStorage.h>
#include <Common/CurrentMetrics.h>
#include <Common/NamePrompter.h>
#include <Interpreters/Context.h>
#include <Common/quoteString.h>
#include <Common/escapeForFileName.h>
#include <Common/filesystemHelpers.h>
#include <Core/Settings.h>
#include <Parsers/formatAST.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>


namespace CurrentMetrics
{
    extern const Metric AttachedDatabase;
}

namespace DB
{

namespace Setting
{
    extern const SettingsBool fsync_metadata;
}

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int CANNOT_BACKUP_TABLE;
    extern const int CANNOT_RESTORE_TABLE;
}

StoragePtr IDatabase::getTable(const String & name, ContextPtr context) const
{
    if (auto storage = tryGetTable(name, context))
        return storage;

    TableNameHints hints(this->shared_from_this(), context);
    /// hint is a pair which holds a single database_name and table_name suggestion for the given table name.
    auto hint = hints.getHintForTable(name);

    if (hint.first.empty())
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} does not exist", backQuoteIfNeed(getDatabaseName()), backQuoteIfNeed(name));
    throw Exception(
        ErrorCodes::UNKNOWN_TABLE,
        "Table {}.{} does not exist. Maybe you meant {}.{}?",
        backQuoteIfNeed(getDatabaseName()),
        backQuoteIfNeed(name),
        backQuoteIfNeed(hint.first),
        backQuoteIfNeed(hint.second));
}

IDatabase::IDatabase(String database_name_) : database_name(std::move(database_name_))
{
    CurrentMetrics::add(CurrentMetrics::AttachedDatabase, 1);
}

IDatabase::~IDatabase()
{
    CurrentMetrics::sub(CurrentMetrics::AttachedDatabase, 1);
}

std::vector<std::pair<ASTPtr, StoragePtr>> IDatabase::getTablesForBackup(const FilterByNameFunction &, const ContextPtr &) const
{
    /// Cannot backup any table because IDatabase doesn't own any tables.
    throw Exception(ErrorCodes::CANNOT_BACKUP_TABLE,
                    "Database engine {} does not support backups, cannot backup tables in database {}",
                    getEngineName(), backQuoteIfNeed(getDatabaseName()));
}

void IDatabase::createTableRestoredFromBackup(const ASTPtr & create_table_query, ContextMutablePtr, std::shared_ptr<IRestoreCoordination>, UInt64)
{
    /// Cannot restore any table because IDatabase doesn't own any tables.
    throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE,
                    "Database engine {} does not support restoring tables, cannot restore table {}.{}",
                    getEngineName(), backQuoteIfNeed(getDatabaseName()),
                    backQuoteIfNeed(create_table_query->as<const ASTCreateQuery &>().getTable()));
}

void IDatabase::persistMetadataImpl(ContextPtr query_context)
{
    auto create_query = getCreateDatabaseQuery()->clone();
    auto * create = create_query->as<ASTCreateQuery>();

    create->attach = true;
    create->if_not_exists = false;

    WriteBufferFromOwnString statement_buf;
    formatAST(*create, statement_buf, false);
    writeChar('\n', statement_buf);
    String statement = statement_buf.str();

    String database_name_escaped = escapeForFileName(TSA_SUPPRESS_WARNING_FOR_READ(database_name));   /// FIXME
    fs::path metadata_root_path = fs::canonical(query_context->getGlobalContext()->getPath());
    fs::path metadata_file_tmp_path = fs::path(metadata_root_path) / "metadata" / (database_name_escaped + ".sql.tmp");
    fs::path metadata_file_path = fs::path(metadata_root_path) / "metadata" / (database_name_escaped + ".sql");

    WriteBufferFromFile out(metadata_file_tmp_path, statement.size(), O_WRONLY | O_CREAT | O_EXCL);
    writeString(statement, out);

    out.next();
    if (query_context->getSettingsRef()[Setting::fsync_metadata])
        out.sync();
    out.close();

    fs::rename(metadata_file_tmp_path, metadata_file_path);
}
}
