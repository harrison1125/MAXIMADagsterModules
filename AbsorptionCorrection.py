"""
Corrects signal for absorption based on sample thickness
"""
def absorption_value(alloy, thickness_mm, wavelength_A):
   return [alloy, thickness_mm, wavelength_A]

if __name__ == "__main__":
    print(absorption_value(1, 1, 1))
