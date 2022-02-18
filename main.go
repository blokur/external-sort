package main

import (
	"fmt"
	"os"
	"time"

	"github.com/askiada/external-sort/file"
	"github.com/askiada/external-sort/internal"
	"github.com/askiada/external-sort/vector"
	"github.com/askiada/external-sort/vector/key"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var log = logrus.StandardLogger()

func main() {
	rootCmd := &cobra.Command{
		Use:   "external-sort",
		Short: "Perform an external sorting on an input file",
		RunE:  rootRun,
	}

	rootCmd.PersistentFlags().StringVarP(&internal.InputFile, internal.InputFileName, "i", viper.GetString(internal.InputFileName), "input file path.")
	rootCmd.PersistentFlags().StringVarP(&internal.OutputFile, internal.OutputFileName, "o", viper.GetString(internal.OutputFileName), "output file path.")
	rootCmd.PersistentFlags().StringVarP(&internal.ChunkFolder, internal.ChunkFolderName, "c", viper.GetString(internal.ChunkFolderName), "chunk folder.")

	rootCmd.PersistentFlags().IntVarP(&internal.ChunkSize, internal.ChunkSizeName, "s", viper.GetInt(internal.ChunkSizeName), "chunk size.")
	rootCmd.PersistentFlags().Int64VarP(&internal.MaxWorkers, internal.MaxWorkersName, "w", viper.GetInt64(internal.MaxWorkersName), "max worker.")
	rootCmd.PersistentFlags().IntVarP(&internal.OutputBufferSize, internal.OutputBufferSizeName, "b", viper.GetInt(internal.OutputBufferSizeName), "output buffer size.")

	fmt.Println("Input file", internal.InputFile)
	fmt.Println("Output file", internal.OutputFile)
	fmt.Println("Chunk foler", internal.ChunkFolder)
	cobra.CheckErr(rootCmd.Execute())
}

func rootRun(cmd *cobra.Command, _ []string) error {
	start := time.Now()
	inputPath := internal.InputFile
	f, err := os.Open(inputPath)
	if err != nil {
		return errors.Wrap(err, "opening input path")
	}
	defer f.Close()
	output, err := os.Create(internal.OutputFile)
	if err != nil {
		return errors.Wrap(err, "creating output file")
	}
	defer func() {
		err := output.Close()
		if err != nil {
			log.Error(err)
		}
	}()
	fI := &file.Info{
		Input: f,
		Allocate: vector.DefaultVector(func(line string) (key.Key, error) {
			return key.AllocateTsv(line, 0)
		}),
		Output:      output,
		ChunkFolder: internal.ChunkFolder,
	}

	err = fI.Sort(cmd.Context(), internal.ChunkSize, int(internal.MaxWorkers), internal.OutputBufferSize)
	if err != nil {
		return errors.Wrap(err, "creating chunks")
	}

	elapsed := time.Since(start)
	fmt.Println(elapsed)
	return nil
}
