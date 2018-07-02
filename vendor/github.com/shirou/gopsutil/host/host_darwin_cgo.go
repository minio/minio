// +build darwin
// +build cgo

package host

// #cgo LDFLAGS: -framework IOKit
// #include "include/smc.c"
import "C"
import "context"

func SensorsTemperatures() ([]TemperatureStat, error) {
	return SensorsTemperaturesWithContext(context.Background())
}

func SensorsTemperaturesWithContext(ctx context.Context) ([]TemperatureStat, error) {
	temperatureKeys := []string{
		C.AMBIENT_AIR_0,
		C.AMBIENT_AIR_1,
		C.CPU_0_DIODE,
		C.CPU_0_HEATSINK,
		C.CPU_0_PROXIMITY,
		C.ENCLOSURE_BASE_0,
		C.ENCLOSURE_BASE_1,
		C.ENCLOSURE_BASE_2,
		C.ENCLOSURE_BASE_3,
		C.GPU_0_DIODE,
		C.GPU_0_HEATSINK,
		C.GPU_0_PROXIMITY,
		C.HARD_DRIVE_BAY,
		C.MEMORY_SLOT_0,
		C.MEMORY_SLOTS_PROXIMITY,
		C.NORTHBRIDGE,
		C.NORTHBRIDGE_DIODE,
		C.NORTHBRIDGE_PROXIMITY,
		C.THUNDERBOLT_0,
		C.THUNDERBOLT_1,
		C.WIRELESS_MODULE,
	}
	var temperatures []TemperatureStat

	C.open_smc()
	defer C.close_smc()

	for _, key := range temperatureKeys {
		temperatures = append(temperatures, TemperatureStat{
			SensorKey:   key,
			Temperature: float64(C.get_tmp(C.CString(key), C.CELSIUS)),
		})
	}
	return temperatures, nil
}
