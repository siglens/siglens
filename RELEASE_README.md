# SigLens Release Instructions

SigLens Release process should generate binaries and docker images for the supported architectures & platforms

SigLens docker image should support: `linux/arm64 and linux/amd64` 

SigLens executable should support: `linux/arm64, linux/amd64, darwin/arm64, and darwin/amd64`

## SigLens Release Steps

1. Create a pull request to develop which increment the verion number `SigLensVersion` located in `pkg/config/version.go` ***This step is critical. Failure to increment this number will lead to failure in creating github release***
2. Merge develop to main using Create a Merge Commit
   Do NOT Squash and Merge
3. GitHub Actions will take care of the following builds:
   1. siglens docker image
      - `linux/amd64, linux/arm64`
      - The docker image build uses buildx to create an image index & the corresponding builds
