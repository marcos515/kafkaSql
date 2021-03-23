# kafka -> SQL

Um script que coleta dados do kafka em json e envia para o SQL, basta configurar o arquivo config.json e kafka.json.
Para mapear as variáveis com a tabela, basta ir até o arquivo config.json e editar a key "binding", colocando valores no formato: ["jsonKey-colunaSql"] É importante separalos pelo hífen.
