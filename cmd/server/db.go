package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // driver "pgx"
	"go.mau.fi/whatsmeow/store/sqlstore"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// dbProvider replica a arquitetura de storage da WAHA em Postgres:
//   - 1 banco PRINCIPAL  ("<ns>_main")      -> tabela `sessions` (config)
//   - 1 banco POR SESSÃO ("<ns>_<id>")      -> store do whatsmeow daquela sessão
//
// Isola cada número (se o banco de uma sessão pifa, não afeta as outras) e
// espalha a carga de escrita — o gargalo de escritor único do SQLite some.
type dbProvider struct {
	base     *url.URL // aponta para o banco de manutenção (ex.: /postgres)
	ns       string   // namespace dos bancos (ex.: "wacalls")
	waLogger waLog.Logger
	log      *slog.Logger

	admin *sql.DB // conexão ao banco de manutenção (CREATE/DROP DATABASE)
	mu    sync.Mutex
}

// newDBProvider conecta ao servidor Postgres (pela URL de manutenção), garante
// o banco principal e devolve o provedor pronto.
func newDBProvider(ctx context.Context, rawURL, ns string, waLogger waLog.Logger, log *slog.Logger) (*dbProvider, error) {
	if rawURL == "" {
		return nil, fmt.Errorf("WACALLS_PG_URL não definida (URL do Postgres é obrigatória)")
	}
	if ns == "" {
		ns = "wacalls"
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("WACALLS_PG_URL inválida: %w", err)
	}
	admin, err := sql.Open("pgx", rawURL)
	if err != nil {
		return nil, fmt.Errorf("abrir conexão admin: %w", err)
	}
	// O Postgres pode subir depois do nosso container (Swarm não espera healthcheck).
	// Tenta por ~60s antes de desistir, em vez de crashar de cara.
	var pingErr error
	for i := 0; i < 30; i++ {
		if pingErr = admin.PingContext(ctx); pingErr == nil {
			break
		}
		log.Warn("aguardando Postgres ficar disponível", "host", u.Host, "tentativa", i+1)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	if pingErr != nil {
		return nil, fmt.Errorf("ping no Postgres (%s): %w", u.Host, pingErr)
	}
	p := &dbProvider{base: u, ns: ns, waLogger: waLogger, log: log, admin: admin}
	if err := p.ensureDatabase(ctx, p.mainDBName()); err != nil {
		return nil, fmt.Errorf("garantir banco principal: %w", err)
	}
	return p, nil
}

func (p *dbProvider) mainDBName() string             { return p.ns + "_main" }
func (p *dbProvider) sessionDBName(id string) string { return p.ns + "_" + id }

// dsnFor devolve a URL de conexão para um banco específico (troca o path).
func (p *dbProvider) dsnFor(dbName string) string {
	u := *p.base
	u.Path = "/" + dbName
	return u.String()
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// ensureDatabase cria o banco se ainda não existir (CREATE DATABASE não aceita
// IF NOT EXISTS, então checamos no catálogo antes).
func (p *dbProvider) ensureDatabase(ctx context.Context, name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var exists bool
	if err := p.admin.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)`, name).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}
	if _, err := p.admin.ExecContext(ctx, `CREATE DATABASE `+quoteIdent(name)); err != nil {
		// corrida: outra goroutine pode ter criado entre o SELECT e o CREATE
		if strings.Contains(err.Error(), "already exists") {
			return nil
		}
		return err
	}
	p.log.Info("banco de sessão criado", "database", name)
	return nil
}

// openMainDB abre a conexão com o banco principal (tabela de config das sessões).
func (p *dbProvider) openMainDB(ctx context.Context) (*sql.DB, error) {
	db, err := sql.Open("pgx", p.dsnFor(p.mainDBName()))
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		return nil, err
	}
	return db, nil
}

// openSessionContainer garante o banco da sessão, abre a conexão e devolve o
// container do whatsmeow já migrado, junto do *sql.DB (para fechar no delete).
func (p *dbProvider) openSessionContainer(ctx context.Context, id string) (*sqlstore.Container, *sql.DB, error) {
	name := p.sessionDBName(id)
	if err := p.ensureDatabase(ctx, name); err != nil {
		return nil, nil, fmt.Errorf("garantir banco da sessão: %w", err)
	}
	db, err := sql.Open("pgx", p.dsnFor(name))
	if err != nil {
		return nil, nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, nil, err
	}
	container := sqlstore.NewWithDB(db, "postgres", p.waLogger)
	if err := container.Upgrade(ctx); err != nil {
		db.Close()
		return nil, nil, fmt.Errorf("migrar store da sessão: %w", err)
	}
	return container, db, nil
}

// dropSessionDB derruba o banco da sessão (chamado no delete da sessão).
func (p *dbProvider) dropSessionDB(ctx context.Context, id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	name := p.sessionDBName(id)
	// FORCE encerra conexões remanescentes (Postgres 13+).
	if _, err := p.admin.ExecContext(ctx, `DROP DATABASE IF EXISTS `+quoteIdent(name)+` WITH (FORCE)`); err != nil {
		return err
	}
	p.log.Info("banco de sessão removido", "database", name)
	return nil
}

func (p *dbProvider) close() {
	if p.admin != nil {
		_ = p.admin.Close()
	}
}
