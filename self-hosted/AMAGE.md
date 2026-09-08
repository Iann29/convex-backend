# AMAGE Convex backend — Candidate 4

## Candidata escolhida

**A candidata é a variante direct, entregue como `capacity`.** Ela serve as HTTP
actions diretamente na porta 3211, mantém a API cloud na 3210 e conserva **16
vagas por padrão**. O knob `SITE_MAX_CONCURRENT_REQUESTS` permite o perfil de 64
vagas testado com PostgreSQL.

| Variante avaliada                                                                | Decisão                                                           |
| -------------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| Pooled: proxy com cliente Hyper compartilhado e keep-alive                       | Alternativa testada; não é a candidata publicada.                 |
| Direct/capacity: listener 3211 atende o router existente, sem forwarding interno | Candidata selecionada. O perfil pesado foi validado nesta imagem. |

O experimento `SQLITE_CACHE_KIB` não integra esta candidata. Não demonstrou
benefício na comparação controlada. Nenhum limite de mutações, regra OCC ou
garantia de durabilidade foi relaxado pelo patch.

## Identidade e procedência

O [manifesto da candidata](amage-candidate4.json) registra a versão, base,
commit de runtime, hash do patch, arquitetura e parâmetros exatos de build. A
publicação reutiliza o artefato validado; não recompila nem altera seus labels.

- Base upstream: `849a6fc605b5a910c9834b61a77c5475cc24d189`.
- Commit dos patches anteriores de SQLite/search/storage/build:
  `d9b60407ca2be1a36e633b3e283d3052c932f3b8`.
- Commit final do runtime C4: `4268bb2a41192924ea68ade229f7303620d672a6`.
- SHA-256 do patch de runtime:
  `a96722fa7fe39f1a542c787894650486b4334e83ea6987e998ec7808820fbb22`.
- Versão compilada:
  `amage-20260907-candidate.4-capacity+base.849a6fc605b5.patch.a96722fa7fe3`.
- Plataforma validada: **Linux/amd64**. Não é uma publicação ARM64.

`org.amage.base-revision` e `org.opencontainers.image.revision` continuam
identificando a base upstream; `org.amage.patch-sha256` identifica as
alterações. O commit de runtime no manifesto faz a ligação com o fork. Os labels
não foram reescritos para sugerir um novo build.

Publicada em 08/09/2026 e puxada novamente pelo digest, com identidade igual ao
artefato testado:

```text
ghcr.io/amageweb/convex-backend@sha256:0fb869fc1a614c68e28cd003ebe17e77642d65a70fcc38d2184cfa01b54e57da
```

Tag:
`ghcr.io/amageweb/convex-backend:amage-20260907-candidate.4-capacity-a96722fa7fe3`.
Usar `digestReference` para fixar a imagem; a tag identifica a candidata, mas
não substitui um digest. O pacote GHCR exige autenticação de uma conta
autorizada.

Para conferir o código, a partir da raiz desta branch:

```bash
git diff --binary --no-ext-diff --no-color --src-prefix=a/ --dst-prefix=b/ \
  --abbrev=9 849a6fc605b5a910c9834b61a77c5475cc24d189 \
  -- crates self-hosted/docker-build/Dockerfile.backend | sha256sum
```

Resultado esperado:
`a96722fa7fe39f1a542c787894650486b4334e83ea6987e998ec7808820fbb22`. Documentação
e ferramentas de teste não entram no hash de runtime.

## Mudanças verificadas

- [SQLite](../crates/sqlite/src/lib.rs): preserva paginação de
  documentos/índices, filtro por tabela, revisões e tombstones. O SQL da página
  de índice percorre a chave ordenada e busca a revisão mais recente por chave,
  evitando agrupar/ordenar o intervalo inteiro a cada página. Não muda o schema
  físico.
- [Listener site](../crates/local_backend/src/site.rs): usa o router HTTP
  existente, timeout, shutdown e identidade de métricas `backend_http_proxy`;
  padrão de 16 vagas. Zero em `SITE_MAX_CONCURRENT_REQUESTS` é rejeitado. As
  portas 3210 e 3211 continuam com papéis separados.
- [Composição](../crates/local_backend/src/main.rs) e
  [router](../crates/local_backend/src/router.rs): compartilham a mesma
  aplicação. O antigo cliente/proxy interno foi removido. Flags de bind/site,
  métodos, streaming e metarrotas foram preservados nos testes.
- [Storage local](../crates/storage/src/lib.rs): atributos usam metadados do
  arquivo, sem ler seu conteúdo inteiro; buffer local limitado a 5 MiB,
  preservando arquivos maiores que 10.000 partes. O multipart S3 não foi
  alterado.
- [Search](../crates/search/src/archive/cache.rs): preserva limpeza de extrações
  incompletas e diretórios readonly sem seguir symlinks, cache configurável e
  leitores de segmentos imutáveis com reload manual.
- [Build](docker-build/Dockerfile.backend): preserva builder Bookworm fixado,
  propagação de versão/base/hash e Node 22.22.2. Cache de search da imagem
  continua em 2.048 MiB.

## Evidência medida e limites

**Meta original, 240 msg/min:** 120 segundos por célula, dois webhooks/mensagem,
três empresas, 3.000 documentos e espera assíncrona de 500 ms por handler.
Backends com quota de 2 CPUs; armazenamento tmpfs. C4 direct: gap p95 de **1,32
ms SQLite / 1,06 ms PostgreSQL**, máximos **2,28 / 1,99 ms**. Todas as 1.920
requisições dessas duas células ficaram abaixo de 20 ms; nenhum evento ausente
ou duplicado. C3 tinha gap p95 de 2,26/2,51 segundos nas mesmas condições.

**SQL isolado:** SQLite 3.46.0, 330 mil chaves, cinco amostras aquecidas,
equivalência de resultados. Páginas de 100 linhas: ASC 57,56→0,92 ms; DESC
97,49→0,94 ms. Isso mede SQL e cópia de resultados, não a latência de um webhook
completo. Os oito testes Rust de paginação/equivalência passaram no patch de SQL
empregado pela C4.

**Carga pesada:** 100 empresas × 50 msg/min, 330 mil documentos iniciais, uma
mutação por webhook. Cada mutação lê o contador da empresa e 100 itens, insere
59 payloads e um receipt e atualiza o contador: aproximadamente **488 linhas
físicas por mensagem**, com OCC ativo. Uma espera assíncrona de 200 ms ocorre
depois da mutação. A contagem paginada confirma todos os IDs e os 100
contadores.

VM Namespace com 16 vCPU/32 GiB, volumes em disco; backend com quota de 6 CPUs/8
GiB, PG com 4 CPUs/4 GiB. Resultados de uma rodada de cinco minutos:

| Métrica                                            | Medido                                  |
| -------------------------------------------------- | --------------------------------------- |
| Webhooks concluídos                                | 50.000, zero erros e contagens corretas |
| HTTP p95                                           | 322,84 ms, incluindo a espera de 200 ms |
| Gap emissor × log p95 / p99 / máximo               | 0,66 / 3,33 / 501,86 ms                 |
| Drenagem após emissão                              | 311,57 ms                               |
| Retentativas OCC                                   | 24.987                                  |
| Checkpoints concluídos na janela                   | 1                                       |
| SIGKILL de backend e PG → recuperação e recontagem | 47,53 s, contagens preservadas          |

Os [resultados estruturados](bench/candidate4-results.json) também incluem os
controles: com quota PG de 2 CPUs, o gap p95 chegou a 1,83 s na janela longa;
com 4 CPUs e LZ4, WAL caiu 6,8%, mas gap p99 e recuperação pioraram nesta
comparação. O perfil selecionado mantém compressão desligada.

**Limites:** dados sintéticos uniformes; não incluem o aplicativo comercial,
workpool/aggregate/IA, ingress externo, TLS ou histórico de 37 GB. Igualar
linhas físicas não iguala UDFs, transações SQL ou custo dos componentes. Uma
rodada por perfil não produz intervalo de confiança. SQLite pesado continuou
acumulando fila; WAL FULL ajudou, sem demonstrar capacidade sustentada de 5.000
msg/min. SIGKILL depois dos acknowledgements não simula perda do host/disco ou
falta de energia.

## Perfil PostgreSQL de laboratório

O perfil validado combina os seguintes valores; não é uma alteração automática
dos defaults da imagem:

| Camada          | Valor                                                                                                                                                            |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Backend         | quota de 6 CPUs; memória 8 GiB                                                                                                                                   |
| Backend env     | `SITE_MAX_CONCURRENT_REQUESTS=64`, `MAX_BYTES_WRITTEN_PER_SECOND=16777216`                                                                                       |
| PostgreSQL 18.6 | quota de 4 CPUs; memória 4 GiB                                                                                                                                   |
| PG parâmetros   | `max_wal_size=8GB`, `shared_buffers=1GB`, `wal_compression=off`                                                                                                  |
| PG preservado   | `synchronous_commit=on`, `fsync=on`, `full_page_writes=on`, `checkpoint_timeout=300s`, `checkpoint_completion_target=0.9`, `work_mem=4MB`, `max_connections=100` |

Quotas são tetos de CPU, não reservas de núcleos. Esse orçamento não equivale a
executar toda a pilha numa máquina compartilhada de 6 vCPU. WAL maior exige
espaço e pode ampliar o replay após queda; `max_wal_size` não é uma cota rígida
de disco. **64 vagas isoladamente pioraram alguns casos; 16 permanece o
padrão.**

As variáveis acima pertencem ao processo/container do backend, não ao ambiente
das funções JavaScript. O modo WAL SQLite, quando ensaiado, foi configurado
offline no arquivo existente com `PRAGMA journal_mode=WAL`, preservando FULL;
não há um novo env para habilitá-lo. Reverter a imagem não reverte esse PRAGMA
persistente.

## Validação reproduzível

O [executor de aceite](test-candidate.mjs) valida identidade e patch contra a
base, SQLite/PG, documentos, bytes de arquivos, Node actions/callbacks,
recuperação após erro, realtime/reconexão, busca, jobs vencidos após restart e
export ZIP com CRC/IDs/hashes. Cria exclusivamente containers/redes/volumes
sintéticos, sem portas publicadas, e os remove ao terminar.

Pré-requisitos: Linux, Docker local acessível via `sudo -n`, Node, Python 3,
`bsdtar`, `nsenter` e uma checkout com CLI/SDK Convex compiladas. O aceite
registrado usou CLI 1.43.0. `CONVEX_CLI_REPO` aponta à raiz dessa checkout,
contendo `npm-packages/convex/dist/cli.bundle.cjs` e
`dist/cjs/browser/index.js`.

```bash
# A partir da raiz do fork; definir o caminho da checkout com CLI compilada.
export CONVEX_CLI_REPO=/path/to/built-convex-checkout
export CANDIDATE_CPUSET=0
AMAGE_IMAGE=$(node -p "require('./self-hosted/amage-candidate4.json').digestReference")
sudo -n docker --host unix:///var/run/docker.sock pull "$AMAGE_IMAGE"
node self-hosted/test-candidate.mjs "$AMAGE_IMAGE"
```

Na preparação para publicação, o aceite completo passou novamente a partir desta
worktree commitada, nos dois motores. A imagem anterior C3 foi rejeitada pelo
guard de identidade antes de criar recursos. O artefato publicado foi puxado
pelo digest e passou probes de ABI, versão do backend e Node.

O smoke do benchmark realocado passou com 16 vagas no CPU lógico 0, incluindo
contagens, SIGKILL, recuperação e análise dos logs. A tentativa com 64 vagas
nesse CPU único atingiu pico de 62 e falhou na asserção de saturação antes da
carga; recursos foram removidos. Ela não foi contada como um benchmark aprovado.
A prova de 64 vagas continua sendo a medição no Namespace com recursos
equivalentes aos descritos acima.

O notebook de desenvolvimento marcou TSC como instável; o aceite local foi
limitado ao CPU lógico 0. A dependência `fastant` também falhou com a C3 nesse
host. No Namespace, 100 partidas de cada C3/direct passaram sem essa restrição.
CPU 0 é uma adaptação do ensaio funcional local, não um perfil de desempenho.

Para repetir o benchmark numa máquina descartável com recursos equivalentes,
definir `CANDIDATE_CPUSET` para os CPUs lógicos disponíveis (no host medido,
`0-15`), usar diretório de saída novo e executar sequencialmente:

```bash
AMAGE_RESULTS=$(mktemp -d /tmp/amage-c4-bench.XXXXXX)
node self-hosted/bench/heavy100.mjs --image "$AMAGE_IMAGE" --engine postgres \
  --backend-cpus 6 --postgres-cpus 4 --site-slots 64 \
  --pg-profile wal8-buffers1g --write-mib 16 --seconds 300 --delay-ms 200 \
  --rate 5000 --seed-docs 330000 --max-sockets 512 --stats \
  --out "$AMAGE_RESULTS/postgres-5k"
python3 self-hosted/bench/analyze-http.py "$AMAGE_RESULTS/postgres-5k"
```

O harness pareia cada `audit_id` com seu log antes dos percentis. O gap inclui a
espera no Agent HTTP; `gapAfterSocketMs` a separa. O log termina nos headers e o
emissor lê o corpo inteiro. Verificar `summary.json`, `requests.json`,
`pg-before-load.json`/`pg-after-load.json`, `crash-recovery.json` e
`cleanup.json`. Gates: 50.000 IDs únicos, 100 contadores corretos, zero erros,
p95 do gap <20 ms, drenagem sem acúmulo crescente e checkpoint concluído dentro
da carga. Guardar também p99/máximo e tempo de recuperação.

## Reconstrução

Os argumentos do build original estão no manifesto. Exemplo equivalente para
Linux/amd64:

```bash
sudo -n docker --host unix:///var/run/docker.sock build --platform linux/amd64 \
  -f self-hosted/docker-build/Dockerfile.backend -t amage-c4:rebuild \
  --build-arg CARGO_BUILD_JOBS=2 \
  --build-arg VERGEN_GIT_SHA=849a6fc605b5a910c9834b61a77c5475cc24d189 \
  --build-arg VERGEN_GIT_COMMIT_TIMESTAMP=2026-09-05T23:58:23Z \
  --build-arg AMAGE_PATCH_SHA256=a96722fa7fe39f1a542c787894650486b4334e83ea6987e998ec7808820fbb22 \
  --build-arg CONVEX_RELEASE_VERSION=amage-20260907-candidate.4-capacity+base.849a6fc605b5.patch.a96722fa7fe3 .
node self-hosted/test-candidate.mjs amage-c4:rebuild
```

APT/runtime ainda têm insumos flutuantes: reconstruir não garante bytes/digest
idênticos. A referência publicada identifica o artefato efetivamente testado.
Publicação da candidata não implica deploy, atualização de catálogo/default,
migração ou validação do aplicativo real.
