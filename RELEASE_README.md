# SigLens Release Instructions

SigLens Release process should generate binaries and docker images for the supported architectures & platforms

SigLens docker image should support: `linux/arm64 and linux/amd64` 

SigLens executable should support: `linux/arm64, linux/amd64, darwin/arm64, and darwin/amd64`

## SigLens Release Steps

1. In the `develop` branch, increment the version number `SigLensVersion` located in `pkg/config/version.go`. The version number in `develop` will have a suffix, for example: “0.1.29d”. 
2. When you are ready to do a release, remove the “d” suffix and create a pull request to merge these changes into `develop`. After merging, `develop` will now have the updated version number without the "d" suffix, for example: “0.1.29”.
***This step is critical. Failure to increment this number will lead to failure in creating a GitHub release.***
2. Add detailed release notes in the pull request describing the changes, enhancements, and bug fixes in this release.
3. Merge develop to main using Create a Merge Commit. Do NOT Squash and Merge.
4. GitHub Actions will take care of the following builds:
   1. siglens docker image
      - `linux/amd64, linux/arm64`
      - The docker image build uses buildx to create an image index & the corresponding builds
5. Once the release completes, increment the version number in develop with a "d" suffix, for example, “0.1.30d”.

Note: The main branch will never have the "d" suffix in the version number. The "d" suffix is only for the develop branch to indicate a development version.