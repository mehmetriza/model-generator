<?php

namespace Reliese\Meta\Sqlserver;

use Illuminate\Support\Arr;
use Reliese\Meta\Blueprint;
use Illuminate\Support\Fluent;
use Illuminate\Database\Connection;
use Illuminate\Database\SqlServerConnection;

class Schema implements \Reliese\Meta\Schema
{
    /**
     * @var string
     */
    protected $schema;

    /**
     * @var \Illuminate\Database\SqlServerConnection
     */
    protected $connection;

    /**
     * @var bool
     */
    protected $loaded = false;

    /**
     * @var \Reliese\Meta\Blueprint[]
     */
    protected $tables = [];

    /**
     * Schema constructor.
     *
     * @param string $schema
     * @param \Illuminate\Database\SqlServerConnection $connection
     */
    public function __construct($schema, SqlServerConnection $connection)
    {
        
        $this->connection = $connection;
        $this->schema = $this->connection->getDatabaseName();

        $this->connection->getDoctrineConnection()->getDatabasePlatform()->registerDoctrineTypeMapping('sysname', 'string');
        $this->connection->getDoctrineConnection()->getDatabasePlatform()->registerDoctrineTypeMapping('xml', 'string');
        

        $types  = $this->connection->select("
            SELECT st2.name AS base_name, st1.name 
            FROM sys.types AS st1
            LEFT JOIN sys.types st2 ON st2.user_type_id = st1.system_type_id AND st2.is_user_defined = 0
            WHERE st1.is_user_defined = 1 OR st1.is_assembly_type = 1
        ");
        $column = new Column();
        foreach ($types as $key => $type) {
            
            foreach ($column::$mappings as $phpType => $database) {
                if (in_array($type->base_name ?: "string", $database)) {
                    $this->connection->getDoctrineConnection()->getDatabasePlatform()->registerDoctrineTypeMapping($type->name, $phpType);
                }
            }
        }
        
        
        

        $this->load();
    }

    public function manager()
    {
        return $this->connection->getDoctrineSchemaManager();
    }

    /**
     * Loads schema's tables' information from the database.
     */
    protected function replaceTableName(string $table)
    {
        return str_replace(".","W",$table);
    }
    protected function revertTableName(string $table)
    {
        return str_replace("W",".",$table);
    }
    protected function load()
    {
        $tables = $this->fetchTables();

        foreach ($tables as $table) {
            
            $blueprint = new Blueprint($this->connection->getName(), $this->schema, $this->replaceTableName($table->getName()));
            $this->fillColumns($blueprint);
            $this->fillConstraints($blueprint);
            $this->tables[ $this->replaceTableName($table->getName()) ] = $blueprint;
            
        }
    }

    /**
     * @return array
     * @internal param string $schema
     */
    protected function fetchTables()
    {
        $names = $this->manager()->listTables();
        
        return $names;
    }

    /**
     * @param \Reliese\Meta\Blueprint $blueprint
     */
    protected function fillColumns(Blueprint $blueprint)
    {
        $columns = $this->manager()->listTableColumns($this->revertTableName($blueprint->table()));
        
        foreach ($columns as $column) {
            $blueprint->withColumn(
                $this->parseColumn($column)
            );
        }
    }

    /**
     * @param \Doctrine\DBAL\Schema\Column $metadata
     *
     * @return \Illuminate\Support\Fluent
     */
    protected function parseColumn($metadata)
    {
        return (new Column($metadata))->normalize();
    }
    
    /**
     * @param \Reliese\Meta\Blueprint $blueprint
     */
    protected function fillConstraints(Blueprint $blueprint)
    {
        $this->fillPrimaryKey($blueprint);
        $this->fillIndexes($blueprint);

        $this->fillRelations($blueprint);
        
    }

    protected function arraify($data)
    {
        return json_decode(json_encode($data), true);
    }
    
     /**
     * @param \Reliese\Meta\Blueprint $blueprint
     * @todo: Support named primary keys
     */
    protected function fillPrimaryKey(Blueprint $blueprint)
    {
        $indexes = $this->manager()->listTableIndexes($this->revertTableName($blueprint->table()));

        $key = [
            'name' => 'primary',
            'index' => '',
            'columns' => optional($indexes['primary']??null)->getColumns()?:[],
        ];

        $blueprint->withPrimaryKey(new Fluent($key));
    }

    /**
     * @param \Reliese\Meta\Blueprint $blueprint
     * @internal param string $sql
     */
    protected function fillIndexes(Blueprint $blueprint)
    {
        $indexes = $this->manager()->listTableIndexes($this->revertTableName($blueprint->table()));
        unset($indexes['primary']);

        foreach ($indexes as $setup) {
            $index = [
                'name' => $setup->isUnique() ? 'unique' : 'index',
                'columns' => $setup->getColumns(),
                'index' => $setup->getName(),
            ];
            $blueprint->withIndex(new Fluent($index));
        }
    }

    /**
     * @param \Reliese\Meta\Blueprint $blueprint
     * @todo: Support named foreign keys
     */
    protected function fillRelations(Blueprint $blueprint)
    {
        $relations = $this->manager()->listTableForeignKeys($this->revertTableName($blueprint->table()));
        
        foreach ($relations as $setup) {
            
            $schema_name = $this->connection->select("select TABLE_SCHEMA from INFORMATION_SCHEMA.TABLES where TABLE_NAME=?",[$setup->getForeignTableName()])[0] ?? "";
            $schema_name = $schema_name->TABLE_SCHEMA ?? "";
            $schema_name = $schema_name == "dbo" ? "" : $schema_name;

            try {
                $foreign_name = implode(".",array_filter([$schema_name, $setup->getForeignTableName()]));
                
                $table = ['database' => $this->schema, 'table'=> $this->replaceTableName($foreign_name)];
    
                $relation = [
                    'name' => 'foreign',
                    'index' => '',
                    'columns' => $setup->getColumns(),
                    'references' => $setup->getForeignColumns(),
                    'on' => $table,
                ];
                $blueprint->withRelation(new Fluent($relation));
                
            } catch (\Throwable $th) {
                //throw $th;
                die('eag');
            }
        }
    }

    /**
     * @param \Illuminate\Database\Connection $connection
     *
     * @return array
     */
    public static function schemas(Connection $connection)
    {
        $schemas = $connection->getDoctrineSchemaManager()->listDatabases();
        

        return array_diff($schemas, [
            'master',
            'tempdb',
            'model',
            'msdb',
            'sysdb'
        ]);
    }

    /**
     * @return string
     */
    public function schema()
    {
        return $this->schema;
    }

    /**
     * @param string $table
     *
     * @return bool
     */
    public function has($table)
    {
        return array_key_exists($table, $this->tables);
    }

    /**
     * @return \Reliese\Meta\Blueprint[]
     */
    public function tables()
    {
        return $this->tables;
    }

    /**
     * @return \Reliese\Meta\Blueprint
     */
    public function table($table)
    {
        $table_name = str_replace(".","W",$table);
        if (! $this->has($table_name)) {
            throw new \InvalidArgumentException("Table [$table_name] does not belong to schema [{$this->schema}]");
        }

        return $this->tables[$table_name];
    }

    /**
     * @return \Illuminate\Database\MySqlConnection
     */
    public function connection()
    {
        return $this->connection;
    }

    /**
     * @param \Reliese\Meta\Blueprint $table
     *
     * @return array
     */
    public function referencing(Blueprint $table)
    {
        $references = [];

        foreach ($this->tables as $blueprint) {
            foreach ($blueprint->references($table) as $reference) {
                $references[] = [
                    'blueprint' => $blueprint,
                    'reference' => $reference,
                ];
            }
        }

        return $references;
    }
}