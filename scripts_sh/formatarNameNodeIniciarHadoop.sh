#!/bin/bash
# ============================================================
# 🚀 Inicializador do HDFS (Hadoop Distributed File System)
# Autor: Charles Junqueira
# ============================================================

HADOOP_HOME="/usr/local/hadoop"
HDFS_BIN="$HADOOP_HOME/bin/hdfs"
SUDO_USER=$(whoami)

echo "=============================================="
echo "   🚀 Iniciando HDFS - Hadoop Data Lake"
echo "   Usuário atual: $SUDO_USER"
echo "=============================================="

# ------------------------------------------------------------
# 1️⃣ Verificar se o usuário é 'hadoop'
# ------------------------------------------------------------
if [ "$SUDO_USER" != "hadoop" ]; then
  echo "⚠️  Este script deve ser executado como usuário 'hadoop'"
  echo "👉 Use:  su - hadoop"
  exit 1
fi

# ------------------------------------------------------------
# 2️⃣ Parar serviços existentes (caso estejam ativos)
# ------------------------------------------------------------
echo "🛑 Parando serviços Hadoop existentes..."
$HADOOP_HOME/sbin/stop-dfs.sh >/dev/null 2>&1
sleep 3

# ------------------------------------------------------------
# 3️⃣ Verificar se o NameNode precisa ser formatado
# ------------------------------------------------------------
NAMENODE_DIR=$(grep -A1 "<name>dfs.namenode.name.dir</name>" $HADOOP_HOME/etc/hadoop/hdfs-site.xml | grep "<value>" | sed -e 's/<[^>]*>//g' | tr -d ' ')
if [ -z "$NAMENODE_DIR" ]; then
  NAMENODE_DIR="/usr/local/hadoop/hadoop_data/hdfs/namenode"
fi

if [ ! -d "$NAMENODE_DIR/current" ]; then
  echo "🧱 Formatando NameNode pela primeira vez..."
  $HDFS_BIN namenode -format -force
else
  echo "✅ NameNode já formatado."
fi

# ------------------------------------------------------------
# 4️⃣ Iniciar o HDFS
# ------------------------------------------------------------
echo "🚀 Iniciando NameNode, DataNode e SecondaryNameNode..."
$HADOOP_HOME/sbin/start-dfs.sh
sleep 3

# ------------------------------------------------------------
# 5️⃣ Verificar status com JPS
# ------------------------------------------------------------
echo "----------------------------------------------"
echo "🔍 Verificando processos Hadoop:"
jps | grep -E "NameNode|DataNode|SecondaryNameNode" || echo "❌ Nenhum processo Hadoop encontrado!"
echo "----------------------------------------------"

# ------------------------------------------------------------
# 6️⃣ Teste rápido de conectividade HDFS
# ------------------------------------------------------------
echo "🧪 Testando acesso ao HDFS..."
$HDFS_BIN dfs -ls / >/dev/null 2>&1

if [ $? -eq 0 ]; then
  echo "✅ HDFS ativo e acessível!"
else
  echo "❌ Falha ao acessar o HDFS. Verifique logs em: $HADOOP_HOME/logs/"
fi

echo "=============================================="
echo "✅ HDFS pronto para uso no seu Data Lake!"
echo "=============================================="
