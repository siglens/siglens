package alertutils

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// NewGormLogrusLogger returns a new gorm/logger.Interface compliant logger that uses logrus for the actual logging
func NewGormLogrusLogger(logger *logrus.Entry, slowLogThreshold time.Duration) logger.Interface {
	return &gormLogger{
		entry:            logger,
		slowLogThreshold: slowLogThreshold,
	}
}

var _ logger.Interface = &gormLogger{}

type gormLogger struct {
	entry            *logrus.Entry
	slowLogThreshold time.Duration
}

func (g *gormLogger) LogMode(level logger.LogLevel) logger.Interface {
	return g
}

func (g *gormLogger) Info(_ context.Context, msg string, data ...interface{}) {
	g.entry.Info(msg, data)
}

func (g *gormLogger) Warn(_ context.Context, msg string, data ...interface{}) {
	g.entry.Warn(msg, data)
}

func (g *gormLogger) Error(_ context.Context, msg string, data ...interface{}) {
	g.entry.Error(msg, data)
}

func (g *gormLogger) Trace(_ context.Context, begin time.Time, fc func() (string, int64), err error) {
	elapsed := time.Since(begin)
	sql, rows := fc()
	duration := float64(elapsed.Nanoseconds()) / 1e6

	switch {
	case err != nil:
		// record not found is an expected error and thus not logged
		if err == gorm.ErrRecordNotFound {
			return
		}

		g.entry.WithFields(logrus.Fields{
			"error":    err,
			"rows":     rows,
			"duration": duration,
		}).Warn(sql)

	case elapsed > g.slowLogThreshold:
		g.entry.WithFields(logrus.Fields{
			"rows":     rows,
			"duration": duration,
		}).Warn(sql)

	default:
		g.entry.WithFields(logrus.Fields{
			"rows":     rows,
			"duration": duration,
			// "file":     utils.FileWithLineNum(),
		}).Debug(sql)
	}
}
