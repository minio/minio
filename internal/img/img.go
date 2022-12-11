package img

import (
	"bufio"
	"bytes"
	"errors"
	"image"
	"image/gif"
	"image/jpeg"
	"image/png"
	"io"
	"strings"

	"github.com/disintegration/imaging"
	"golang.org/x/image/bmp"
	"golang.org/x/image/tiff"
)

func ScaleImage(width, height int, rd io.Reader) (*bytes.Buffer, error) {
	img, format, err := image.Decode(bufio.NewReader(rd))
	if err != nil {
		return nil, err
	}

	if height == 0 && width == 0 {
		return nil, errors.New("no scale")
	}

	newImage := imaging.Resize(img, width, height, imaging.Lanczos)
	buffer := new(bytes.Buffer)

	switch strings.ToLower(format) {
	case "png":
		if err := png.Encode(buffer, newImage); err != nil {
			return nil, err
		}
	case "bmp":
		if err := bmp.Encode(buffer, newImage); err != nil {
			return nil, err
		}
	case "gif":
		if err := gif.Encode(buffer, newImage, nil); err != nil {
			return nil, err
		}
	case "jpeg", "jpg":
		if err := jpeg.Encode(buffer, newImage, nil); err != nil {
			return nil, err
		}
	case "tiff":
		if err := tiff.Encode(buffer, newImage, nil); err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("unknown format")
	}

	return buffer, nil
}
