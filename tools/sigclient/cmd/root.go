// Copyright (c) 2021-2024 SigScalr, Inc.
//
// This file is part of SigLens Observability Solution
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var generatorType string

var rootCmd = &cobra.Command{
	Use:   "sigscalr-client",
	Short: "sigscalr client",
	Long:  `Client to send data to sigscalr and other related storages`,
	Run: func(cmd *cobra.Command, args []string) {
		if generatorType != "" {
			handleGenerator()
			return
		}
		fmt.Println("Use -h to see available commands")
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func handleGenerator() {
	switch generatorType {
	case "k8s":
		fmt.Println("Kubernetes metrics generator is initialized via the ingest command.")
	default:
		fmt.Printf("Unsupported generator type: %s\n", generatorType)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&generatorType, "generator", "g", "",
		"Metrics generator type (k8s)")
}
